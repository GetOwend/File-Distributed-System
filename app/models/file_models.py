# app/models/file_models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, BigInteger, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .database import Base
from datetime import datetime
import json

class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)

    # File identification
    filename = Column(String(255), nullable=False, index=True)  # Unique stored name
    original_filename = Column(String(255), nullable=False)  # Original upload name

    # File content
    file_hash = Column(String(64), nullable=False, index=True)
    size = Column(BigInteger, nullable=False)  # Original file size in bytes
    stored_size = Column(BigInteger, nullable=True)  # Actual stored size (with replication)

    __table_args__ = (
    UniqueConstraint('file_hash', 'owner_id', 'is_deleted',
                     name='uq_file_hash_owner_not_deleted'),
    )

    # File info
    content_type = Column(String(100), nullable=True)
    file_extension = Column(String(50), nullable=True)

    # DFS-specific fields (ADD THESE)
    chunk_count = Column(Integer, default=1)
    chunk_size = Column(BigInteger, nullable=True)  # Size per chunk in bytes
    replication_factor = Column(Integer, default=1)
    is_chunked = Column(Boolean, default=False)

    # Storage info
    storage_nodes = Column(Text, nullable=True)  # JSON array of node names (legacy)
    chunk_locations = Column(Text, nullable=True)  # JSON mapping of chunk indices to node IDs

    # Ownership
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False, index=True)

    # Status
    is_deleted = Column(Boolean, default=False)
    is_encrypted = Column(Boolean, default=False)
    encryption_key_id = Column(String(100), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_accessed = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    owner = relationship("User", back_populates="files")
    chunks = relationship("FileChunk", back_populates="file", cascade="all, delete-orphan")

    def get_storage_nodes_list(self):
        """Parse storage nodes from JSON"""
        if self.storage_nodes:
            return json.loads(self.storage_nodes)
        return []

    def get_chunk_locations(self):
        """Parse chunk locations from JSON"""
        if self.chunk_locations:
            return json.loads(self.chunk_locations)
        return {}

    def __repr__(self):
        return f"<FileMetadata(id={self.id}, filename='{self.filename}', size={self.size})>"

class FileChunk(Base):
    """Tracks individual file chunks across storage nodes"""
    __tablename__ = "file_chunks"

    id = Column(Integer, primary_key=True, index=True)

    # File reference
    file_hash = Column(String(64), ForeignKey('file_metadata.file_hash'), index=True, nullable=False)
    chunk_index = Column(Integer, nullable=False)  # 0, 1, 2, ...

    # Chunk identification
    chunk_hash = Column(String(64), nullable=False, index=True)
    size = Column(BigInteger, nullable=False)  # Chunk size in bytes

    # Storage locations
    primary_node_id = Column(Integer, ForeignKey('storage_nodes.id'), nullable=False)
    backup_node_ids = Column(Text, nullable=True)  # JSON array of backup node IDs

    # Additional storage info
    stored_on_nodes = Column(Text, nullable=True)  # JSON array of all nodes where chunk is stored
    storage_paths = Column(Text, nullable=True)  # JSON dict of {node_id: path}

    # Status
    is_stored = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_verified = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    file = relationship("FileMetadata", back_populates="chunks")
    primary_node = relationship("StorageNode", foreign_keys=[primary_node_id])

    def get_backup_node_ids(self):
        """Parse backup node IDs from JSON"""
        if self.backup_node_ids:
            return json.loads(self.backup_node_ids)
        return []

    def get_stored_on_nodes(self):
        """Parse stored nodes from JSON"""
        if self.stored_on_nodes:
            return json.loads(self.stored_on_nodes)
        return []

    def __repr__(self):
        return f"<FileChunk(file_hash={self.file_hash[:8]}, index={self.chunk_index}, size={self.size})>"

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