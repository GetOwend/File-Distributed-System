# app/routes/files.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import StreamingResponse, FileResponse as FastAPIFileResponse
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
import os
import hashlib
import json
import logging
import uuid
import io
from datetime import datetime
import asyncio
from app.models.database import get_db
from app.models.file_models import FileMetadata, FileChunk, StorageNode
from app.models.user_models import User
from app.models.schemas import (
    FileResponse, FileUploadResponse, FileListResponse,
    FileDeleteResponse, FileDetailResponse, ChunkInfo,
    FileUpdateResponse,
)
from app.config.settings import settings
from app.utils.auth import get_current_user
from app.utils.file_chunker import split_file_into_chunks
from app.utils.node_selector import get_available_storage_nodes, select_nodes_for_chunks
from app.utils.chunk_storage import (
    retrieve_chunk_from_node,
    store_chunk_on_node,
    write_chunk_quorum,
    read_chunk_quorum,
    delete_chunk_locally,
)
from app.utils.cleanup import cleanup_temp_files
from app.utils.repair import repair_file_by_id

logger = logging.getLogger(__name__)
router = APIRouter()

# Ensure storage directory exists
os.makedirs(settings.STORAGE_PATH, exist_ok=True)

def get_available_storage_nodes(db: Session) -> List[StorageNode]:
    """Get active storage nodes for file distribution"""
    return db.query(StorageNode).filter(
        StorageNode.is_active == True,
        StorageNode.health_status.in_(["healthy", "offline"])
    ).all()

@router.post("/files/upload", response_model=FileUploadResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    chunk_size_mb: int = Query(5, description="Chunk size in MB (default: 5)"),
    replication_factor: int = Query(5, description="Number of replicas per chunk (default: 3)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Upload a file to the distributed file system with chunking and replication"""
    try:
        # Check file size
        file_content = await file.read()
        file_size = len(file_content)

        if file_size > settings.MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File size exceeds maximum limit of {settings.MAX_FILE_SIZE} bytes"
            )

        # Check user quota
        user_used_storage = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).with_entities(func.sum(FileMetadata.size)).scalar() or 0

        if user_used_storage + file_size > current_user.storage_quota:
            raise HTTPException(
                status_code=400,
                detail="Storage quota exceeded"
            )

        # Generate file hash
        file_hash = hashlib.sha256(file_content).hexdigest()

        # Check for duplicate files
        existing_file = db.query(FileMetadata).filter(
            FileMetadata.file_hash == file_hash,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if existing_file:
            return FileUploadResponse(
                message="File already exists",
                file_id=existing_file.id,
                filename=existing_file.original_filename,
                size=existing_file.size,
                file_hash=existing_file.file_hash,
                chunk_count=existing_file.chunk_count or 1
            )

        # Save file temporarily for chunking
        temp_dir = f"/tmp/upload_{uuid.uuid4().hex}"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, file.filename)

        with open(temp_file_path, "wb") as f:
            f.write(file_content)

        # 1. SPLIT FILE INTO CHUNKS
        chunks = split_file_into_chunks(temp_file_path, chunk_size_mb)
        if not chunks:
            raise HTTPException(status_code=500, detail="Failed to split file into chunks")

        # 2. SELECT STORAGE NODES FOR EACH CHUNK
        storage_nodes = get_available_storage_nodes(db)
        if not storage_nodes:
            raise HTTPException(
                status_code=503,
                detail="No storage nodes available"
            )

        chunk_assignments = select_nodes_for_chunks(
            db=db,
            num_chunks=len(chunks),
            replication_factor=replication_factor
        )

        # 3. DISTRIBUTE CHUNKS TO NODES using quorum writes
        chunk_metadata_records = []
        total_stored_size = 0
        nodes_used = set()
        # Track per-node usage increments to update atomically later
        node_usage_increments = {}
        # Track successes per chunk in case we need to cleanup on failure
        stored_chunks_successes = []

        # quorum write threshold
        w = max(1, (replication_factor // 2) + 1)

        for i, chunk_data in enumerate(chunks):
            chunk_hash = hashlib.sha256(chunk_data).hexdigest()

            # Get assigned nodes for this chunk
            assigned_node_ids = chunk_assignments[i]

            # Build StorageNode objects list
            node_objs = []
            for node_id in assigned_node_ids:
                storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
                if storage_node:
                    node_objs.append(storage_node)

            # Perform quorum write
            success_node_ids = write_chunk_quorum(
                chunk_data=chunk_data,
                chunk_index=i,
                nodes=node_objs,
                file_hash=file_hash,
                chunk_hash=chunk_hash,
                w=w,
                timeout=30
            )

            if not success_node_ids or len(success_node_ids) < w:
                # Cleanup any previously stored chunks for this file
                for prev_index, prev_nodes in stored_chunks_successes:
                    for nid in prev_nodes:
                        node_row = db.query(StorageNode).filter(StorageNode.id == nid).first()
                        if node_row:
                            try:
                                delete_chunk_locally(node_row.node_path, file_hash, prev_index)
                            except Exception:
                                pass

                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to reach write quorum for chunk {i} (needed {w})"
                )

            # Record successes for cleanup if needed later
            stored_chunks_successes.append((i, success_node_ids))

            # Create chunk metadata based on successful nodes
            primary_node = success_node_ids[0] if success_node_ids else None
            backup_ids_for_meta = success_node_ids[1:] if len(success_node_ids) > 1 else []

            chunk_metadata = FileChunk(
                file_hash=file_hash,
                chunk_index=i,
                chunk_hash=chunk_hash,
                size=len(chunk_data),
                primary_node_id=primary_node,
                backup_node_ids=json.dumps(backup_ids_for_meta) if backup_ids_for_meta else None,
                stored_on_nodes=json.dumps(success_node_ids),
                is_stored=True
            )
            chunk_metadata_records.append(chunk_metadata)

            # Update totals and node usage increments
            total_stored_size += len(chunk_data) * len(success_node_ids)
            for nid in success_node_ids:
                nodes_used.add(nid)
                node_usage_increments[nid] = node_usage_increments.get(nid, 0) + len(chunk_data)

        # 4. CREATE FILE METADATA
        unique_filename = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{file.filename}"
        db_file = FileMetadata(
            filename=unique_filename,
            original_filename=file.filename,
            file_hash=file_hash,
            size=file_size,
            content_type=file.content_type or "application/octet-stream",
            file_extension=os.path.splitext(file.filename)[1],
            owner_id=current_user.id,
            chunk_count=len(chunks),
            replication_factor=replication_factor,
            chunk_size=chunk_size_mb * 1024 * 1024,
            stored_size=total_stored_size,  # Actual stored size (with replication)
            is_chunked=True
        )

        # Apply node usage updates and persist chunk metadata and file metadata atomically
        try:
            # Lock and update storage node usage
            for node_id, inc in node_usage_increments.items():
                node_row = db.query(StorageNode).filter(StorageNode.id == node_id).with_for_update().first()
                if node_row:
                    node_row.used_space = (node_row.used_space or 0) + inc
                    node_row.available_space = max(0, node_row.total_space - node_row.used_space)
                    db.add(node_row)

            # Add chunk metadata records
            for cm in chunk_metadata_records:
                db.add(cm)

            # Add file metadata
            db.add(db_file)

            # Commit the transaction explicitly to avoid nested begin() on the session
            db.commit()

        except Exception as e:
            logger.error(f"DB transaction failed during upload metadata persist: {str(e)}")
            db.rollback()
            raise

        # Refresh db_file after commit
        db.refresh(db_file)

        # 5. CLEANUP TEMP FILES
        background_tasks.add_task(cleanup_temp_files, temp_dir)

        logger.info(f"File uploaded with DFS: {file.filename} ({len(chunks)} chunks) by user {current_user.username}")

        return FileUploadResponse(
            message="File uploaded successfully with distributed storage",
            file_id=db_file.id,
            filename=file.filename,
            size=file_size,
            file_hash=file_hash,
            chunk_count=len(chunks),
            nodes_used=list(nodes_used),
            replication_factor=replication_factor
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading file with DFS: {str(e)}")
        raise HTTPException(status_code=500, detail="File upload failed")

@router.get("/files", response_model=FileListResponse)
async def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all files for the current user"""
    try:
        # Get total count
        total_count = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).count()

        # Get files with pagination
        files = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).order_by(FileMetadata.created_at.desc()).offset(skip).limit(limit).all()

        file_responses = [
            FileResponse(
                id=file.id,
                filename=file.original_filename,
                size=file.size,
                created_at=file.created_at,
                is_chunked=file.is_chunked,
                chunk_count=file.chunk_count,
                replication_factor=file.replication_factor
            )
            for file in files
        ]

        return FileListResponse(
            files=file_responses,
            total_count=total_count
        )

    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving files")

@router.get("/files/{file_id}/download")
async def download_file(
    file_id: int,
    current_user: Optional[User] = Depends(get_current_user),  # Make optional
    db: Session = Depends(get_db)
):
    """Download a file - allows public access for public files"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Check access permissions
        is_owner = current_user and file_metadata.owner_id == current_user.id
        can_access = is_owner or file_metadata.is_public

        if not can_access:
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to access this file"
            )

        # Update last accessed time
        file_metadata.last_accessed = datetime.utcnow()
        db.commit()

        # Check if file is chunked - if not, we can't download it anymore
        if not file_metadata.is_chunked:
            raise HTTPException(
                status_code=410,  # 410 Gone - file format no longer supported
                detail="Legacy file format not supported. Please re-upload the file."
            )

        # Get all chunks for this file, ordered by chunk_index
        chunks = db.query(FileChunk).filter(
            FileChunk.file_hash == file_metadata.file_hash
        ).order_by(FileChunk.chunk_index).all()

        if not chunks:
            raise HTTPException(
                status_code=404,
                detail="No chunks found for file"
            )

        # Verify all chunks are stored
        missing_chunks = [c.chunk_index for c in chunks if not c.is_stored]
        if missing_chunks:
            raise HTTPException(
                status_code=500,
                detail=f"Missing or corrupted chunks: {missing_chunks}"
            )

        # Verify total size matches expected
        total_chunk_size = sum(c.size for c in chunks)
        if total_chunk_size != file_metadata.size:
            logger.warning(
                f"File size mismatch for {file_metadata.filename}: "
                f"metadata={file_metadata.size}, chunks={total_chunk_size}"
            )

        # --- Pre-check quorum availability for all chunks (fail before streaming starts) ---
        for chunk_meta in chunks:
            try:
                if chunk_meta.stored_on_nodes:
                    node_ids = json.loads(chunk_meta.stored_on_nodes)
                else:
                    node_ids = []
                    if chunk_meta.primary_node_id:
                        node_ids.append(chunk_meta.primary_node_id)
                    if chunk_meta.backup_node_ids:
                        node_ids.extend(json.loads(chunk_meta.backup_node_ids))

                r = max(1, (file_metadata.replication_factor // 2) + 1)
                check = read_chunk_quorum(
                    chunk_index=chunk_meta.chunk_index,
                    file_hash=file_metadata.file_hash,
                    node_ids=node_ids,
                    r=r,
                    timeout=10
                )
                if check is None:
                    logger.error(f"Quorum pre-check failed for chunk {chunk_meta.chunk_index} (r={r})")
                    raise HTTPException(status_code=503, detail=f"Failed to retrieve chunk {chunk_meta.chunk_index} with quorum r={r}")
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Unexpected error during quorum pre-check for chunk {chunk_meta.chunk_index}: {str(e)}")
                raise HTTPException(status_code=500, detail="Error during pre-check for file download")

        # --- Stream chunks (generator is best-effort; on error stop streaming gracefully) ---
        async def chunk_generator():
            for chunk_meta in chunks:
                # compute node_ids and r
                if chunk_meta.stored_on_nodes:
                    node_ids = json.loads(chunk_meta.stored_on_nodes)
                else:
                    node_ids = []
                    if chunk_meta.primary_node_id:
                        node_ids.append(chunk_meta.primary_node_id)
                    if chunk_meta.backup_node_ids:
                        node_ids.extend(json.loads(chunk_meta.backup_node_ids))

                r = max(1, (file_metadata.replication_factor // 2) + 1)

                try:
                    chunk_data = read_chunk_quorum(
                        chunk_index=chunk_meta.chunk_index,
                        file_hash=file_metadata.file_hash,
                        node_ids=node_ids,
                        r=r,
                        timeout=10
                    )

                    if chunk_data is None:
                        logger.error(f"Quorum read failed during streaming for chunk {chunk_meta.chunk_index} (r={r})")
                        return

                    # Verify chunk hash for integrity
                    if hashlib.sha256(chunk_data).hexdigest() != chunk_meta.chunk_hash:
                        logger.error(f"Chunk {chunk_meta.chunk_index} hash mismatch during streaming")
                        return

                    # Verify chunk size
                    if len(chunk_data) != chunk_meta.size:
                        logger.warning(
                            f"Chunk size mismatch for {chunk_meta.chunk_index}: "
                            f"expected={chunk_meta.size}, actual={len(chunk_data)}"
                        )

                    yield chunk_data

                except Exception as e:
                    logger.error(f"Error retrieving chunk {chunk_meta.chunk_index} during streaming: {str(e)}")
                    return

        # Return streaming response
        return StreamingResponse(
            chunk_generator(),
            media_type=file_metadata.content_type or "application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={file_metadata.original_filename}",
                "Content-Length": str(file_metadata.size)
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail="File download failed")

@router.delete("/files/{file_id}", response_model=FileDeleteResponse)
async def delete_file(
    file_id: int,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a file and its chunks from storage nodes"""
    try:
        # Get file metadata FIRST (before passing to background task)
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Get all chunks for this file
        chunks = db.query(FileChunk).filter(
            FileChunk.file_hash == file_metadata.file_hash
        ).all()

        # Soft delete file metadata
        file_metadata.is_deleted = True
        file_metadata.deleted_at = datetime.utcnow()

        # Collect chunk information BEFORE committing
        chunk_data = []
        for chunk in chunks:
            chunk_data.append({
                "chunk_index": chunk.chunk_index,
                "stored_on_nodes": json.loads(chunk.stored_on_nodes) if chunk.stored_on_nodes else [],
                "file_hash": chunk.file_hash
            })
            chunk.is_stored = False

        db.commit()

        # Schedule actual chunk deletion in background
        background_tasks.add_task(
            cleanup_file_chunks_task,
            file_hash=file_metadata.file_hash,
            chunk_data=chunk_data  # Pass pre-collected data
        )

        logger.info(f"File marked for deletion: {file_metadata.original_filename} by user {current_user.username}")

        return FileDeleteResponse(
            message="File deleted successfully",
            file_id=file_id,
            filename=file_metadata.original_filename
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting file: {str(e)}")
        raise HTTPException(status_code=500, detail="File deletion failed")

async def cleanup_file_chunks_task(file_hash: str, chunk_data: list):
    """Background task to delete chunks from storage nodes"""
    # Create a NEW database session for the background task
    from app.models.database import SessionLocal

    db = SessionLocal()
    try:
        logger.info(f"Starting chunk cleanup for file hash: {file_hash[:8]}...")

        # Delete chunks from storage nodes
        deleted_count = 0
        for chunk_info in chunk_data:
            stored_nodes = chunk_info["stored_on_nodes"]

            for node_id in stored_nodes:
                storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
                if storage_node:
                    # Delete chunk from node
                    success = delete_chunk_from_node(
                        chunk_index=chunk_info["chunk_index"],
                        file_hash=file_hash,
                        node_path=storage_node.node_path
                    )

                    if success:
                        # Update node storage usage
                        chunk_size = 0  # We'd need to store this, but for now estimate
                        storage_node.used_space = max(0, (storage_node.used_space or 0) - chunk_size)
                        storage_node.available_space = min(
                            storage_node.total_space,
                            (storage_node.available_space or 0) + chunk_size
                        )
                        deleted_count += 1

        # Delete chunk metadata from database
        db.query(FileChunk).filter(FileChunk.file_hash == file_hash).delete()
        db.commit()

        logger.info(f"Cleaned up {deleted_count} chunks for file hash: {file_hash[:8]}...")

    except Exception as e:
        db.rollback()
        logger.error(f"Error in background chunk cleanup: {str(e)}")
    finally:
        db.close()

def delete_chunk_from_node(chunk_index: int, file_hash: str, node_path: str) -> bool:
    """Delete a chunk from a storage node's filesystem"""
    try:
        chunks_dir = os.path.join(node_path, "chunks")

        if not os.path.exists(chunks_dir):
            logger.warning(f"Chunks directory not found: {chunks_dir}")
            return False

        # Look for chunk file
        import glob
        chunk_pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}_*.chunk")
        chunk_files = glob.glob(chunk_pattern)

        if not chunk_files:
            # Try without wildcard
            chunk_pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}.chunk")
            chunk_files = glob.glob(chunk_pattern)

        deleted = 0
        for chunk_path in chunk_files:
            try:
                os.remove(chunk_path)
                deleted += 1
                logger.debug(f"Deleted chunk file: {chunk_path}")
            except Exception as e:
                logger.error(f"Error deleting {chunk_path}: {str(e)}")

        return deleted > 0

    except Exception as e:
        logger.error(f"Error in delete_chunk_from_node: {str(e)}")
        return False

@router.get("/files/{file_id}/info", response_model=FileDetailResponse)
async def get_file_info(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific file including chunk info"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).options(joinedload(FileMetadata.chunks)).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Get chunk information
        chunks_info = []
        if file_metadata.is_chunked:
            for chunk in file_metadata.chunks:
                backup_ids = json.loads(chunk.backup_node_ids) if chunk.backup_node_ids else []
                chunks_info.append(ChunkInfo(
                    chunk_index=chunk.chunk_index,
                    chunk_hash=chunk.chunk_hash[:16] + "...",  # Shorten for display
                    size=chunk.size,
                    primary_node_id=chunk.primary_node_id,
                    backup_node_ids=backup_ids,
                    is_stored=chunk.is_stored
                ))

        return FileDetailResponse(
            id=file_metadata.id,
            filename=file_metadata.original_filename,
            size=file_metadata.size,
            stored_size=file_metadata.stored_size,
            file_hash=file_metadata.file_hash,
            chunk_count=file_metadata.chunk_count,
            replication_factor=file_metadata.replication_factor,
            is_chunked=file_metadata.is_chunked,
            content_type=file_metadata.content_type,
            created_at=file_metadata.created_at.isoformat(),
            chunks=chunks_info
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting file info: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving file information")

@router.get("/files/{file_id}/chunks/{chunk_index}")
async def download_chunk(
    file_id: int,
    chunk_index: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Download a specific chunk of a file (for debugging/testing). Verify that replication works."""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        if not file_metadata.is_chunked:
            raise HTTPException(status_code=400, detail="File is not chunked")

        # Get chunk metadata
        chunk_meta = db.query(FileChunk).filter(
            FileChunk.file_hash == file_metadata.file_hash,
            FileChunk.chunk_index == chunk_index
        ).first()

        if not chunk_meta:
            raise HTTPException(status_code=404, detail="Chunk not found")

        # Attempt quorum read for the chunk
        if chunk_meta.stored_on_nodes:
            node_ids = json.loads(chunk_meta.stored_on_nodes)
        else:
            node_ids = []
            if chunk_meta.primary_node_id:
                node_ids.append(chunk_meta.primary_node_id)
            if chunk_meta.backup_node_ids:
                node_ids.extend(json.loads(chunk_meta.backup_node_ids))

        r = max(1, (file_metadata.replication_factor // 2) + 1)
        chunk_data = read_chunk_quorum(
            chunk_index=chunk_index,
            file_hash=file_metadata.file_hash,
            node_ids=node_ids,
            r=r,
            timeout=10
        )

        if chunk_data is None:
            raise HTTPException(status_code=404, detail="Chunk not found on any storage node (quorum failure)")

        # Verify hash
        if hashlib.sha256(chunk_data).hexdigest() != chunk_meta.chunk_hash:
            raise HTTPException(status_code=500, detail="Chunk hash mismatch")

        # Return chunk as file
        filename = f"{file_metadata.original_filename}.chunk{chunk_index}"
        return StreamingResponse(
            io.BytesIO(chunk_data),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading chunk: {str(e)}")
        raise HTTPException(status_code=500, detail="Chunk download failed")

@router.put("/files/{file_id}", response_model=FileUpdateResponse)
async def update_file(
    file_id: int,
    background_tasks: BackgroundTasks,
    new_file: Optional[UploadFile] = File(None),
    new_filename: Optional[str] = Query(None, description="Optional new filename"),
    description: Optional[str] = Query(None, description="Optional description"),
    chunk_size_mb: int = Query(5, description="Chunk size in MB"),
    replication_factor: int = Query(2, description="Number of replicas per chunk"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):

    try:
        # Get the current (latest) version of the file
        current_file = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False,
            FileMetadata.is_latest_version == True
        ).first()

        if not current_file:
            raise HTTPException(status_code=404, detail="File not found")

        # CASE 1: File content replacement
        if new_file:
            # Read new file content
            file_content = await new_file.read()
            file_size = len(file_content)
            file_hash = hashlib.sha256(file_content).hexdigest()

            # Check if new file is same as current (no-op)
            if file_hash == current_file.file_hash:
                # Same content, just update metadata if needed
                if new_filename:
                    current_file.original_filename = new_filename
                db.commit()

                return FileUpdateResponse(
                    message="File content unchanged, metadata updated",
                    file_id=current_file.id,
                    filename=new_filename or current_file.original_filename,
                    size=current_file.size,
                    version=current_file.version
                )

            # Check quota (include new file size, subtract old if replacing)
            user_used_storage = db.query(func.sum(FileMetadata.size)).filter(
                FileMetadata.owner_id == current_user.id,
                FileMetadata.is_deleted == False,
                FileMetadata.is_latest_version == True
            ).scalar() or 0

            # For quota: new total = current total - old file + new file
            new_total_storage = user_used_storage - current_file.size + file_size
            if new_total_storage > current_user.storage_quota:
                raise HTTPException(
                    status_code=400,
                    detail="Storage quota exceeded with new file version"
                )

            # Save file temporarily for chunking
            temp_dir = f"/tmp/update_{uuid.uuid4().hex}"
            os.makedirs(temp_dir, exist_ok=True)
            temp_file_path = os.path.join(temp_dir, new_file.filename)

            with open(temp_file_path, "wb") as f:
                f.write(file_content)

            # 1. Split file into chunks
            chunks = split_file_into_chunks(temp_file_path, chunk_size_mb)
            if not chunks:
                raise HTTPException(status_code=500, detail="Failed to split file into chunks")

            # 2. Select storage nodes for each chunk
            storage_nodes = get_available_storage_nodes(db)
            if not storage_nodes:
                raise HTTPException(
                    status_code=503,
                    detail="No storage nodes available"
                )

            chunk_assignments = select_nodes_for_chunks(
                db=db,
                num_chunks=len(chunks),
                replication_factor=replication_factor
            )

            # 3. Distribute new chunks to nodes
            chunk_metadata_records = []
            total_stored_size = 0
            nodes_used = set()
            node_usage_increments = {}

            # Use quorum writes for update as well
            stored_chunks_successes = []
            w = max(1, (replication_factor // 2) + 1)

            for i, chunk_data in enumerate(chunks):
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                assigned_node_ids = chunk_assignments[i]

                node_objs = []
                for node_id in assigned_node_ids:
                    storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
                    if storage_node:
                        node_objs.append(storage_node)

                success_node_ids = write_chunk_quorum(
                    chunk_data=chunk_data,
                    chunk_index=i,
                    nodes=node_objs,
                    file_hash=file_hash,
                    chunk_hash=chunk_hash,
                    w=w,
                    timeout=30
                )

                if not success_node_ids or len(success_node_ids) < w:
                    # cleanup previous written chunks for this update
                    for prev_index, prev_nodes in stored_chunks_successes:
                        for nid in prev_nodes:
                            node_row = db.query(StorageNode).filter(StorageNode.id == nid).first()
                            if node_row:
                                try:
                                    delete_chunk_locally(node_row.node_path, file_hash, prev_index)
                                except Exception:
                                    pass

                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to reach write quorum for chunk {i} (needed {w})"
                    )

                stored_chunks_successes.append((i, success_node_ids))

                primary_node = success_node_ids[0] if success_node_ids else None
                backup_ids_for_meta = success_node_ids[1:] if len(success_node_ids) > 1 else []

                chunk_metadata = FileChunk(
                    file_hash=file_hash,
                    chunk_index=i,
                    chunk_hash=chunk_hash,
                    size=len(chunk_data),
                    primary_node_id=primary_node,
                    backup_node_ids=json.dumps(backup_ids_for_meta) if backup_ids_for_meta else None,
                    stored_on_nodes=json.dumps(success_node_ids),
                    is_stored=True
                )
                chunk_metadata_records.append(chunk_metadata)

                total_stored_size += len(chunk_data) * len(success_node_ids)
                for nid in success_node_ids:
                    nodes_used.add(nid)
                    node_usage_increments[nid] = node_usage_increments.get(nid, 0) + len(chunk_data)

            # 4. Create NEW file metadata entry (new version)
            unique_filename = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{new_file.filename}"

            new_version = FileMetadata(
                filename=unique_filename,
                original_filename=new_filename or new_file.filename,
                file_hash=file_hash,
                size=file_size,
                content_type=new_file.content_type or "application/octet-stream",
                file_extension=os.path.splitext(new_file.filename)[1],
                owner_id=current_user.id,
                chunk_count=len(chunks),
                replication_factor=replication_factor,
                chunk_size=chunk_size_mb * 1024 * 1024,
                stored_size=total_stored_size,
                is_chunked=True,
                version=current_file.version + 1,
                previous_version_id=current_file.id,
                is_latest_version=True
            )
            # Persist changes atomically: update node usage, add new chunks and file version,
            # and mark old version as not latest
            try:
                # update node usage with SELECT FOR UPDATE
                for node_id, inc in node_usage_increments.items():
                    node_row = db.query(StorageNode).filter(StorageNode.id == node_id).with_for_update().first()
                    if node_row:
                        node_row.used_space = (node_row.used_space or 0) + inc
                        node_row.available_space = max(0, node_row.total_space - node_row.used_space)
                        db.add(node_row)

                # add chunk metadata for new version
                for cm in chunk_metadata_records:
                    db.add(cm)

                # add new version and mark old as not latest
                db.add(new_version)
                current_file.is_latest_version = False

                # Commit explicitly (avoid nested begin())
                db.commit()

            except Exception as e:
                logger.error(f"DB transaction failed during file update persist: {str(e)}")
                db.rollback()
                raise

            # 6. Cleanup temp files
            background_tasks.add_task(cleanup_temp_files, temp_dir)

            # 7. Schedule garbage collection of old chunks (optional)
            background_tasks.add_task(
                schedule_garbage_collection,
                old_file_hash=current_file.file_hash
            )

            db.refresh(new_version)

            logger.info(f"File updated to version {new_version.version}: {new_file.filename}")

            return FileUpdateResponse(
                message="File updated successfully (new version created)",
                file_id=new_version.id,
                filename=new_version.original_filename,
                size=new_version.size,
                version=new_version.version,
                previous_version_id=current_file.id
            )

        # CASE 2: Only metadata update (rename, description, etc.)
        else:
            updates_made = False

            if new_filename and new_filename != current_file.original_filename:
                current_file.original_filename = new_filename
                updates_made = True
                logger.info(f"Renamed file {current_file.id} to {new_filename}")

            if updates_made:
                db.commit()
                return FileUpdateResponse(
                    message="File metadata updated",
                    file_id=current_file.id,
                    filename=current_file.original_filename,
                    size=current_file.size,
                    version=current_file.version
                )
            else:
                return FileUpdateResponse(
                    message="No changes made",
                    file_id=current_file.id,
                    filename=current_file.original_filename,
                    size=current_file.size,
                    version=current_file.version
                )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating file: {str(e)}")
        raise HTTPException(status_code=500, detail="File update failed")

async def schedule_garbage_collection(old_file_hash: str):
    """
    Schedule deletion of old file chunks after a delay
    This allows time for any ongoing operations to complete
    """
    import time
    from app.models.database import SessionLocal

    # Wait before garbage collection (this could honestly be anything from minutes to hours ;) )
    await asyncio.sleep(3600)  # 1 hour delay

    db = SessionLocal()
    try:

        #delete if no active file references this hash
        active_files = db.query(FileMetadata).filter(
            FileMetadata.file_hash == old_file_hash,
            FileMetadata.is_deleted == False,
            FileMetadata.is_latest_version == True
        ).count()

        if active_files == 0:
            # No active files reference this hash, safe to delete chunks
            chunks = db.query(FileChunk).filter(
                FileChunk.file_hash == old_file_hash
            ).all()

            deleted_count = 0
            for chunk in chunks:
                stored_nodes = json.loads(chunk.stored_on_nodes) if chunk.stored_on_nodes else []

                for node_id in stored_nodes:
                    storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
                    if storage_node:
                        delete_chunk_from_node(
                            chunk_index=chunk.chunk_index,
                            file_hash=old_file_hash,
                            node_path=storage_node.node_path
                        )
                        deleted_count += 1

            # Delete chunk metadata
            db.query(FileChunk).filter(FileChunk.file_hash == old_file_hash).delete()
            db.commit()

            logger.info(f"Garbage collected {deleted_count} chunks for old file hash: {old_file_hash[:8]}")
        else:
            logger.info(f"Skipping garbage collection for {old_file_hash[:8]} - still referenced by active files")

    except Exception as e:
        logger.error(f"Error in garbage collection: {str(e)}")
    finally:
        db.close()


@router.post("/files/{file_id}/repair")
async def trigger_repair(
    file_id: int,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Trigger repair/healing for a specific file (runs in background)."""
    try:
        file_meta = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_meta:
            raise HTTPException(status_code=404, detail="File not found")

        background_tasks.add_task(repair_file_by_id, file_id)

        return {"message": "Repair scheduled", "file_id": file_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scheduling repair: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to schedule repair")

@router.get("/files/{file_id}/versions")
async def get_file_versions(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all versions of a file"""
    try:
        # Get the current file to find its version chain
        current_file = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not current_file:
            raise HTTPException(status_code=404, detail="File not found")

        # Find all versions by walking the version chain
        versions = []
        file_to_check = current_file

        # First, find the latest version if this isn't it
        if not file_to_check.is_latest_version:
            latest = db.query(FileMetadata).filter(
                FileMetadata.previous_version_id == file_to_check.id,
                FileMetadata.owner_id == current_user.id,
                FileMetadata.is_deleted == False
            ).first()
            if latest:
                file_to_check = latest

        # Now collect all versions in the chain
        while file_to_check:
            versions.append({
                "id": file_to_check.id,
                "filename": file_to_check.original_filename,
                "size": file_to_check.size,
                "version": file_to_check.version,
                "created_at": file_to_check.created_at.isoformat(),
                "is_latest_version": file_to_check.is_latest_version,
                "file_hash": file_to_check.file_hash[:16] + "..."
            })

            # Move to previous version
            if file_to_check.previous_version_id:
                file_to_check = db.query(FileMetadata).filter(
                    FileMetadata.id == file_to_check.previous_version_id,
                    FileMetadata.owner_id == current_user.id,
                    FileMetadata.is_deleted == False
                ).first()
            else:
                break

        # Sort by version number (descending)
        versions.sort(key=lambda x: x["version"], reverse=True)

        return {
            "file_id": current_file.id,
            "current_filename": current_file.original_filename,
            "total_versions": len(versions),
            "versions": versions
        }

    except Exception as e:
        logger.error(f"Error getting file versions: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving file versions")