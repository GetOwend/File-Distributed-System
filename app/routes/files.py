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
from pathlib import Path

from app.models.database import get_db
from app.models.file_models import FileMetadata, FileChunk, StorageNode
from app.models.user_models import User
from app.models.schemas import (
    FileResponse, FileUploadResponse, FileListResponse,
    FileDeleteResponse, FileDownloadResponse, FileDetailResponse, ChunkInfo
)
from app.config.settings import settings
from app.utils.auth import get_current_user
from app.utils.file_chunker import split_file_into_chunks
from app.utils.node_selector import get_available_storage_nodes, select_nodes_for_chunks
from app.utils.chunk_storage import retrieve_chunk_from_node, store_chunk_on_node
from app.utils.cleanup import cleanup_temp_files

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
    replication_factor: int = Query(2, description="Number of replicas per chunk (default: 2)"),
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

        # 3. DISTRIBUTE CHUNKS TO NODES
        chunk_metadata_records = []
        total_stored_size = 0
        nodes_used = set()

        for i, chunk_data in enumerate(chunks):
            chunk_hash = hashlib.sha256(chunk_data).hexdigest()

            # Get assigned nodes for this chunk
            assigned_node_ids = chunk_assignments[i]

            # Store chunk on each assigned node
            stored_on_nodes = []
            chunk_stored_successfully = False

            for node_id in assigned_node_ids:
                storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
                if not storage_node:
                    continue

                # Store chunk on node
                success = store_chunk_on_node(
                    chunk_data=chunk_data,
                    chunk_index=i,
                    node=storage_node,
                    file_hash=file_hash,
                    chunk_hash=chunk_hash
                )

                if success:
                    stored_on_nodes.append(node_id)
                    total_stored_size += len(chunk_data)
                    chunk_stored_successfully = True
                    nodes_used.add(node_id)

                    # Update node storage usage
                    storage_node.used_space = (storage_node.used_space or 0) + len(chunk_data)
                    storage_node.available_space = max(0, storage_node.total_space - storage_node.used_space)

            # Create chunk metadata
            chunk_metadata = FileChunk(
                file_hash=file_hash,
                chunk_index=i,
                chunk_hash=chunk_hash,
                size=len(chunk_data),
                primary_node_id=assigned_node_ids[0] if assigned_node_ids else None,
                backup_node_ids=json.dumps(assigned_node_ids[1:]) if len(assigned_node_ids) > 1 else None,
                stored_on_nodes=json.dumps(stored_on_nodes),
                is_stored=chunk_stored_successfully
            )
            db.add(chunk_metadata)
            chunk_metadata_records.append(chunk_metadata)

            # If chunk storage failed, raise error
            if not chunk_stored_successfully:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to store chunk {i} on any storage node"
                )

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
        db.add(db_file)

        # Commit all changes
        db.commit()
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
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Download a file - now ONLY handles chunked files"""
    try:
        # Get file metadata with chunks
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

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

        # Create a generator to stream chunks
        async def chunk_generator():
            for chunk_meta in chunks:
                try:
                    # Retrieve chunk from primary node first
                    chunk_data = retrieve_chunk_from_node(
                        chunk_index=chunk_meta.chunk_index,
                        file_hash=file_metadata.file_hash,
                        node_id=chunk_meta.primary_node_id,
                        db=db
                    )

                    # If primary fails, try backup nodes
                    if chunk_data is None and chunk_meta.backup_node_ids:
                        backup_ids = json.loads(chunk_meta.backup_node_ids)
                        for backup_id in backup_ids:
                            chunk_data = retrieve_chunk_from_node(
                                chunk_index=chunk_meta.chunk_index,
                                file_hash=file_metadata.file_hash,
                                node_id=backup_id,
                                db=db
                            )
                            if chunk_data:
                                logger.info(
                                    f"Used backup node {backup_id} for chunk "
                                    f"{chunk_meta.chunk_index} of {file_metadata.filename}"
                                )
                                break

                    if chunk_data is None:
                        raise Exception(f"Failed to retrieve chunk {chunk_meta.chunk_index}")

                    # Verify chunk hash for integrity
                    if hashlib.sha256(chunk_data).hexdigest() != chunk_meta.chunk_hash:
                        raise Exception(f"Chunk {chunk_meta.chunk_index} hash mismatch")

                    # Verify chunk size
                    if len(chunk_data) != chunk_meta.size:
                        logger.warning(
                            f"Chunk size mismatch for {chunk_meta.chunk_index}: "
                            f"expected={chunk_meta.size}, actual={len(chunk_data)}"
                        )

                    yield chunk_data

                except Exception as e:
                    logger.error(f"Error retrieving chunk {chunk_meta.chunk_index}: {str(e)}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to retrieve chunk {chunk_meta.chunk_index}"
                    )

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
    """Download a specific chunk of a file (for debugging/testing)"""
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

        # Retrieve chunk data
        chunk_data = retrieve_chunk_from_node(
            chunk_index=chunk_index,
            file_hash=file_metadata.file_hash,
            node_id=chunk_meta.primary_node_id,
            db=db
        )

        if chunk_data is None and chunk_meta.backup_node_ids:
            # Try backup nodes
            backup_ids = json.loads(chunk_meta.backup_node_ids)
            for backup_id in backup_ids:
                chunk_data = retrieve_chunk_from_node(
                    chunk_index=chunk_index,
                    file_hash=file_metadata.file_hash,
                    node_id=backup_id,
                    db=db
                )
                if chunk_data:
                    break

        if chunk_data is None:
            raise HTTPException(status_code=404, detail="Chunk not found on any storage node")

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