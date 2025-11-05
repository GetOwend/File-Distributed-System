# app/routes/metadata.py
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
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

@router.get("/metadata/files/{file_id}", response_model=FileMetadataDetail)
async def get_file_metadata(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed metadata for a specific file"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Parse storage nodes
        storage_nodes = []
        if file_metadata.storage_nodes:
            try:
                storage_nodes = json.loads(file_metadata.storage_nodes)
            except json.JSONDecodeError:
                storage_nodes = [file_metadata.storage_nodes]

        # Get file chunks if any
        chunks = db.query(FileChunk).filter(
            FileChunk.file_id == file_id
        ).all()

        return FileMetadataDetail(
            id=file_metadata.id,
            filename=file_metadata.original_filename,
            storage_filename=file_metadata.filename,
            size=file_metadata.size,
            file_path=file_metadata.file_path,
            file_hash=file_metadata.file_hash,
            content_type=file_metadata.content_type,
            file_extension=file_metadata.file_extension,
            replication_factor=file_metadata.replication_factor,
            storage_nodes=storage_nodes,
            is_compressed=file_metadata.is_compressed,
            is_public=file_metadata.is_public,
            permissions=file_metadata.permissions,
            chunks=[
                {
                    "chunk_index": chunk.chunk_index,
                    "chunk_size": chunk.chunk_size,
                    "chunk_hash": chunk.chunk_hash,
                    "storage_node": chunk.storage_node_id
                }
                for chunk in chunks
            ],
            created_at=file_metadata.created_at,
            updated_at=file_metadata.updated_at,
            last_accessed=file_metadata.last_accessed
        )

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

        if update_data.permissions:
            file_metadata.permissions = update_data.permissions

        db.commit()
        db.refresh(file_metadata)

        logger.info(f"File metadata updated: {file_metadata.filename} by user {current_user.username}")

        return FileResponse(
            id=file_metadata.id,
            filename=file_metadata.original_filename,
            size=file_metadata.size,
            file_path=file_metadata.file_path,
            created_at=file_metadata.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating file metadata: {str(e)}")
        raise HTTPException(status_code=500, detail="Error updating file metadata")

@router.get("/metadata/search", response_model=FileSearchResponse)
async def search_files(
    query: str = Query(..., description="Search term for filename"),
    file_type: Optional[str] = Query(None, description="File extension filter"),
    min_size: Optional[int] = Query(None, ge=0, description="Minimum file size in bytes"),
    max_size: Optional[int] = Query(None, ge=0, description="Maximum file size in bytes"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Search files by various criteria"""
    try:
        # Base query
        search_query = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        )

        # Apply search filters
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

        # Get paginated results
        files = search_query.order_by(FileMetadata.created_at.desc()).offset(skip).limit(limit).all()

        file_responses = [
            FileResponse(
                id=file.id,
                filename=file.original_filename,
                size=file.size,
                file_path=file.file_path,
                created_at=file.created_at
            )
            for file in files
        ]

        return FileSearchResponse(
            files=file_responses,
            total_count=total_count,
            query=query,
            filters={
                "file_type": file_type,
                "min_size": min_size,
                "max_size": max_size
            }
        )

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