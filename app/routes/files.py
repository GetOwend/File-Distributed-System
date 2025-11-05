# app/routes/files.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import os
import hashlib
import json
import logging
from datetime import datetime

from app.models.database import get_db
from app.models.file_models import FileMetadata, FileChunk, StorageNode
from app.models.user_models import User
from app.models.schemas import (
    FileResponse, FileUploadResponse, FileListResponse,
    FileDeleteResponse, FileDownloadResponse
)
from app.config.settings import settings
from app.utils.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

# Ensure storage directory exists
os.makedirs(settings.STORAGE_PATH, exist_ok=True)

def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of a file"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def get_available_storage_nodes(db: Session) -> List[StorageNode]:
    """Get active storage nodes for file distribution"""
    return db.query(StorageNode).filter(
        StorageNode.is_active == True,
        StorageNode.health_status == 'healthy'
    ).all()

@router.post("/files/upload", response_model=FileUploadResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Upload a file to the distributed file system"""
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
        ).with_entities(FileMetadata.size).all()

        total_used = sum([size[0] for size in user_used_storage]) if user_used_storage else 0
        if total_used + file_size > current_user.storage_quota:
            raise HTTPException(
                status_code=400,
                detail="Storage quota exceeded"
            )

        # Generate unique filename
        file_extension = os.path.splitext(file.filename)[1]
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        unique_filename = f"{timestamp}_{file.filename}"
        file_path = os.path.join(settings.STORAGE_PATH, unique_filename)

        # Save file to storage
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Calculate file hash
        file_hash = calculate_file_hash(file_path)

        # Check for duplicate files
        existing_file = db.query(FileMetadata).filter(
            FileMetadata.file_hash == file_hash,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if existing_file:
            # Remove the newly uploaded file as duplicate exists
            os.remove(file_path)
            return FileUploadResponse(
                message="File already exists",
                file_id=existing_file.id,
                filename=existing_file.filename,
                size=existing_file.size
            )

        # Get storage nodes for replication
        storage_nodes = get_available_storage_nodes(db)
        node_locations = [node.node_name for node in storage_nodes[:2]]  # Use first 2 nodes

        # Create file metadata
        db_file = FileMetadata(
            filename=unique_filename,
            original_filename=file.filename,
            file_path=file_path,
            file_hash=file_hash,
            size=file_size,
            content_type=file.content_type or "application/octet-stream",
            file_extension=file_extension,
            owner_id=current_user.id,
            storage_nodes=json.dumps(node_locations),
            replication_factor=len(node_locations)
        )

        db.add(db_file)
        db.commit()
        db.refresh(db_file)

        logger.info(f"File uploaded successfully: {file.filename} by user {current_user.username}")

        return FileUploadResponse(
            message="File uploaded successfully",
            file_id=db_file.id,
            filename=file.filename,
            size=file_size
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading file: {str(e)}")
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
                file_path=file.file_path,
                created_at=file.created_at
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
    """Download a specific file"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        if not os.path.exists(file_metadata.file_path):
            raise HTTPException(status_code=404, detail="File not found on storage")

        # Update last accessed time
        file_metadata.last_accessed = datetime.utcnow()
        db.commit()

        return FileResponse(
            path=file_metadata.file_path,
            filename=file_metadata.original_filename,
            media_type=file_metadata.content_type
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail="File download failed")

@router.delete("/files/{file_id}", response_model=FileDeleteResponse)
async def delete_file(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Soft delete a file"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

        # Soft delete
        file_metadata.is_deleted = True
        file_metadata.deleted_at = datetime.utcnow()
        db.commit()

        # Note: Actual file deletion could be handled by a background cleanup task

        logger.info(f"File deleted: {file_metadata.filename} by user {current_user.username}")

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

@router.get("/files/{file_id}/info", response_model=FileResponse)
async def get_file_info(
    file_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific file"""
    try:
        file_metadata = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")

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
        logger.error(f"Error getting file info: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving file information")