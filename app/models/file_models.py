# app/models/file_models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .database import Base
from datetime import datetime

class FileMetadata(Base):
    """
    Model for storing file metadata in the distributed file system
    """
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)  # Path in storage system
    file_hash = Column(String(64), nullable=False, index=True)  # SHA-256 hash
    size = Column(BigInteger, nullable=False)  # File size in bytes
    content_type = Column(String(100), nullable=False)

    # File categorization
    file_extension = Column(String(10), nullable=False)
    is_directory = Column(Boolean, default=False)
    parent_directory_id = Column(Integer, ForeignKey('files.id'), nullable=True)

    # Replication and distribution
    replication_factor = Column(Integer, default=2)  # Number of replicas
    storage_nodes = Column(Text, nullable=False)  # JSON string of node locations
    is_compressed = Column(Boolean, default=False)

    # Ownership and permissions
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    is_public = Column(Boolean, default=False)
    permissions = Column(String(10), default='644')  # Unix-style permissions

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_accessed = Column(DateTime(timezone=True), server_default=func.now())

    # Soft delete
    is_deleted = Column(Boolean, default=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    owner = relationship("User", back_populates="files")
    parent_directory = relationship("FileMetadata", remote_side=[id], backref="subdirectories")

    def __repr__(self):
        return f"<FileMetadata(id={self.id}, filename='{self.filename}', size={self.size})>"

class FileChunk(Base):
    """
    Model for storing file chunk information (for large file splitting)
    """
    __tablename__ = "file_chunks"

    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    chunk_index = Column(Integer, nullable=False)  # Order of chunk
    chunk_hash = Column(String(64), nullable=False)  # Hash of individual chunk
    chunk_size = Column(Integer, nullable=False)  # Size of this chunk
    storage_node_id = Column(String(100), nullable=False)  # Which node stores this chunk
    chunk_path = Column(String(500), nullable=False)  # Path to chunk on storage node

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    file = relationship("FileMetadata", backref="chunks")

    def __repr__(self):
        return f"<FileChunk(file_id={self.file_id}, chunk_index={self.chunk_index})>"

class StorageNode(Base):
    """
    Model for tracking storage nodes in the distributed system
    """
    __tablename__ = "storage_nodes"

    id = Column(Integer, primary_key=True, index=True)
    node_name = Column(String(100), unique=True, nullable=False)
    node_url = Column(String(255), nullable=False)  # Base URL of storage node
    node_path = Column(String(500), nullable=False)  # Storage path on node

    # Node status and capacity
    total_space = Column(BigInteger, nullable=False)  # Total space in bytes
    used_space = Column(BigInteger, default=0)  # Used space in bytes
    available_space = Column(BigInteger, nullable=False)  # Available space in bytes

    # Node health and status
    is_active = Column(Boolean, default=True)
    last_heartbeat = Column(DateTime(timezone=True), server_default=func.now())
    health_status = Column(String(20), default='healthy')  # healthy, warning, critical

    # Node metadata
    location = Column(String(100), nullable=True)  # Geographic location
    priority = Column(Integer, default=1)  # Load balancing priority

    # Timestamps
    registered_at = Column(DateTime(timezone=True), server_default=func.now())
    last_updated = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<StorageNode(id={self.id}, node_name='{self.node_name}', status='{self.health_status}')>"