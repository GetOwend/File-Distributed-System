# app/routes/metadata.py
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
import json
import logging

from app.models.database import get_db
from app.models.file_models import FileMetadata, FileChunk
from app.models.user_models import User
from app.models.schemas import (
    FileResponse, FileSearchResponse, FileMetadataDetail,
    FileUpdateRequest, MetadataResponse
)
from app.utils.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/metadata/files/{file_id}")
async def get_file_metadata(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get file metadata and chunk information"""
    try:
        # Get file metadata with chunks
        file_metadata = db.query(FileMetadata).options(
            joinedload(FileMetadata.chunks)
        ).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        response = {
            "file": {
                "id": file_metadata.id,
                "filename": file_metadata.original_filename,
                "stored_filename": file_metadata.filename,
                "file_hash": file_metadata.file_hash,
                "size": file_metadata.size,
                "stored_size": file_metadata.stored_size,
                "chunk_count": file_metadata.chunk_count,
                "replication_factor": file_metadata.replication_factor,
                "is_chunked": file_metadata.is_chunked,
                "content_type": file_metadata.content_type,
                "file_extension": file_metadata.file_extension,
                "created_at": file_metadata.created_at.isoformat() if file_metadata.created_at else None,
                "updated_at": file_metadata.updated_at.isoformat() if file_metadata.updated_at else None,
                "last_accessed": file_metadata.last_accessed.isoformat() if file_metadata.last_accessed else None,
                "version": file_metadata.version,
                "is_latest_version": file_metadata.is_latest_version,
                "previous_version_id": file_metadata.previous_version_id,
                "is_encrypted": file_metadata.is_encrypted
            },
            "chunks": []
        }

        # Add chunk information if file is chunked
        if file_metadata.is_chunked and file_metadata.chunks:
            for chunk in sorted(file_metadata.chunks, key=lambda x: x.chunk_index):
                chunk_data = {
                    "chunk_index": chunk.chunk_index,
                    "chunk_hash": chunk.chunk_hash,
                    "size": chunk.size,
                    "primary_node_id": chunk.primary_node_id,
                    "is_stored": chunk.is_stored,
                    "created_at": chunk.created_at.isoformat() if chunk.created_at else None,
                    "last_verified": chunk.last_verified.isoformat() if chunk.last_verified else None
                }

                # Add backup nodes if available
                if chunk.backup_node_ids:
                    try:
                        chunk_data["backup_node_ids"] = json.loads(chunk.backup_node_ids)
                    except:
                        chunk_data["backup_node_ids"] = []
                else:
                    chunk_data["backup_node_ids"] = []

                # Add stored nodes if available
                if chunk.stored_on_nodes:
                    try:
                        chunk_data["stored_on_nodes"] = json.loads(chunk.stored_on_nodes)
                    except:
                        chunk_data["stored_on_nodes"] = []
                else:
                    chunk_data["stored_on_nodes"] = []

                # Add storage paths if available
                if chunk.storage_paths:
                    try:
                        chunk_data["storage_paths"] = json.loads(chunk.storage_paths)
                    except:
                        chunk_data["storage_paths"] = {}
                else:
                    chunk_data["storage_paths"] = {}

                response["chunks"].append(chunk_data)

        # Add storage node information
        storage_nodes_info = []
        if file_metadata.storage_nodes:
            try:
                node_names = json.loads(file_metadata.storage_nodes)
                for node_name in node_names:
                    storage_node = db.query(StorageNode).filter(
                        StorageNode.node_name == node_name
                    ).first()
                    if storage_node:
                        storage_nodes_info.append({
                            "id": storage_node.id,
                            "name": storage_node.node_name,
                            "url": storage_node.node_url,
                            "health_status": storage_node.health_status
                        })
            except:
                pass

        response["storage_nodes"] = storage_nodes_info

        # Add chunk locations if available
        if file_metadata.chunk_locations:
            try:
                response["chunk_locations"] = json.loads(file_metadata.chunk_locations)
            except:
                response["chunk_locations"] = {}
        else:
            response["chunk_locations"] = {}

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting file metadata: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving file metadata")

@router.put("/metadata/files/{file_id}", response_model=FileResponse)
async def update_file_metadata(
    file_id: int,
    update_data: FileUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update file metadata (permissions, public status, etc.)"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Update allowed fields
        if update_data.is_public is not None:
            file_metadata.is_public = update_data.is_public
            if update_data.is_public and not file_metadata.public_url:
                # Generate public URL when making file public
                file_metadata.generate_public_url()
            elif not update_data.is_public:
                # Clear public URL when making file private
                file_metadata.public_url = None

        if update_data.permissions:
            file_metadata.permissions = json.dumps(update_data.permissions)

        if update_data.original_filename:
            file_metadata.original_filename = update_data.original_filename

        db.commit()
        db.refresh(file_metadata)

        logger.info(f"File metadata updated: {file_metadata.filename} by user {current_user.username}")

        return FileResponse(
            id=file_metadata.id,
            filename=file_metadata.original_filename,
            size=file_metadata.size,
            created_at=file_metadata.created_at,
            is_public=file_metadata.is_public,
            is_chunked=file_metadata.is_chunked,
            chunk_count=file_metadata.chunk_count
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating file metadata: {str(e)}")
        raise HTTPException(status_code=500, detail="Error updating file metadata")

@router.get("/metadata/search", response_model=FileSearchResponse)
async def search_files(
    query: Optional[str] = Query(None, description="Search term for filename"),
    file_type: Optional[str] = Query(None, description="File extension filter"),
    min_size: Optional[int] = Query(None, ge=0, description="Minimum file size in bytes"),
    max_size: Optional[int] = Query(None, ge=0, description="Maximum file size in bytes"),
    show_public_only: bool = Query(False, description="Show only public files"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Optional[User] = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Search files by various criteria

    Behavior:
    1. If authenticated: shows user's files + public files from others
    2. If no parameters: returns all accessible files
    3. Public files appear in search for all authenticated users
    """
    try:
        if not current_user:
            # If not authenticated, only show public files
            show_public_only = True

        # Base query: Get files that are not deleted
        search_query = db.query(FileMetadata).filter(
            FileMetadata.is_deleted == False,
            FileMetadata.is_latest_version == True  # Only show latest versions
        )

        # If authenticated user exists, show their files + public files
        if current_user:
            search_query = search_query.filter(
                # User's own files OR public files
                (FileMetadata.owner_id == current_user.id) |
                (FileMetadata.is_public == True)
            )
        else:
            # Not authenticated, only show public files
            search_query = search_query.filter(FileMetadata.is_public == True)

        # If explicitly requesting only public files
        if show_public_only:
            search_query = search_query.filter(FileMetadata.is_public == True)

        # Apply search filters if provided
        if query:
            search_query = search_query.filter(
                FileMetadata.original_filename.ilike(f"%{query}%")
            )

        if file_type:
            search_query = search_query.filter(
                FileMetadata.file_extension == file_type.lower()
            )

        if min_size is not None:
            search_query = search_query.filter(FileMetadata.size >= min_size)

        if max_size is not None:
            search_query = search_query.filter(FileMetadata.size <= max_size)

        # Get total count
        total_count = search_query.count()

        # Get paginated results with owner information
        files = search_query.options(
            joinedload(FileMetadata.owner)  # Load owner info for public files
        ).order_by(
            FileMetadata.created_at.desc()
        ).offset(skip).limit(limit).all()

        # Prepare response
        file_responses = []
        for file in files:
            is_owner = current_user and file.owner_id == current_user.id

            file_responses.append({
                "id": file.id,
                "filename": file.original_filename,
                "size": file.size,
                "created_at": file.created_at,
                "is_public": file.is_public,
                "is_chunked": file.is_chunked,
                "chunk_count": file.chunk_count,
                "replication_factor": file.replication_factor,
                "content_type": file.content_type,
                "owner": {
                    "id": file.owner.id,
                    "username": file.owner.username,
                    "is_current_user": is_owner
                } if file.owner else None,
                "can_download": is_owner or file.is_public,
                "can_edit": is_owner  # Only owners can edit
            })

        return {
            "files": file_responses,
            "total_count": total_count,
            "query": query,
            "filters": {
                "file_type": file_type,
                "min_size": min_size,
                "max_size": max_size,
                "show_public_only": show_public_only
            },
            "user_context": {
                "authenticated": current_user is not None,
                "user_id": current_user.id if current_user else None
            }
        }

    except Exception as e:
        logger.error(f"Error searching files: {str(e)}")
        raise HTTPException(status_code=500, detail="Error searching files")

@router.get("/metadata/stats", response_model=MetadataResponse)
async def get_metadata_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get statistics about user's files and storage"""
    try:
        from sqlalchemy import func, distinct

        # Basic stats
        total_files = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).count()

        total_size = db.query(func.coalesce(func.sum(FileMetadata.size), 0)).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).scalar()

        # Files by type
        files_by_type = db.query(
            FileMetadata.file_extension,
            func.count(FileMetadata.id),
            func.coalesce(func.sum(FileMetadata.size), 0)
        ).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).group_by(FileMetadata.file_extension).all()

        # Recent activity
        recent_files = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).order_by(FileMetadata.last_accessed.desc()).limit(10).all()

        # Storage distribution
        storage_nodes_usage = {}
        files = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).all()

        for file in files:
            if file.storage_nodes:
                try:
                    nodes = json.loads(file.storage_nodes)
                    for node in nodes:
                        storage_nodes_usage[node] = storage_nodes_usage.get(node, 0) + (file.size / len(nodes))
                except json.JSONDecodeError:
                    pass

        return MetadataResponse(
            total_files=total_files,
            total_size=total_size,
            files_by_type={
                ext: {"count": count, "total_size": size}
                for ext, count, size in files_by_type
            },
            storage_distribution=storage_nodes_usage,
            recent_files=[
                {
                    "filename": file.original_filename,
                    "size": file.size,
                    "last_accessed": file.last_accessed
                }
                for file in recent_files
            ]
        )

    except Exception as e:
        logger.error(f"Error getting metadata stats: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving metadata statistics")