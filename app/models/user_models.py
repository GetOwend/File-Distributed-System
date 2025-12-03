# app/models/user_models.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, BigInteger, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .database import Base
from datetime import datetime

class User(Base):
    """
    Model for system users and authentication
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    # User identification
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)

    # Authentication
    hashed_password = Column(String(255), nullable=False)
    salt = Column(String(50), nullable=False)  # For password hashing

    # User profile
    full_name = Column(String(100), nullable=True)
    storage_quota = Column(BigInteger, default=1073741824)  # 1GB default quota in bytes
    used_storage = Column(BigInteger, default=0)  # Used storage in bytes

    # Account status
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)

    # Security
    last_login = Column(DateTime(timezone=True), nullable=True)
    failed_login_attempts = Column(Integer, default=0)
    account_locked_until = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    files = relationship("FileMetadata", back_populates="owner")
    sessions = relationship("UserSession", back_populates="user")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"

class UserSession(Base):
    """
    Model for tracking user sessions and API tokens
    """
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)

    # Session identification
    session_token = Column(String(255), unique=True, nullable=False, index=True)
    refresh_token = Column(String(255), unique=True, nullable=True, index=True)

    # Session metadata
    user_agent = Column(Text, nullable=True)
    ip_address = Column(String(45), nullable=True)  # Supports IPv6

    # Session validity
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_active = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_accessed = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="sessions")

    def __repr__(self):
        return f"<UserSession(id={self.id}, user_id={self.user_id}, is_active={self.is_active})>"

class UserQuota(Base):
    """
    Model for tracking user storage quotas and usage
    """
    __tablename__ = "user_quotas"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), unique=True, nullable=False)

    # Quota limits
    max_storage_bytes = Column(BigInteger, default=1073741824)  # 1GB default
    max_files = Column(Integer, default=1000)  # Maximum number of files

    # Current usage
    used_storage_bytes = Column(BigInteger, default=0)
    used_file_count = Column(Integer, default=0)

    # Quota alerts
    alert_threshold = Column(Integer, default=90)  # Percentage
    last_alert_sent = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship("User")

    def __repr__(self):
        return f"<UserQuota(user_id={self.user_id}, used={self.used_storage_bytes}/{self.max_storage_bytes})>"

class BlacklistedToken(Base):
    """
    Model for tracking blacklisted/invalidated JWT tokens
    """
    __tablename__ = "blacklisted_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String(500), unique=True, index=True, nullable=False)
    token_hash = Column(String(64), index=True, nullable=False)  # SHA256 hash for indexing
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)

    # Token metadata
    jti = Column(String(36), nullable=True, index=True)  # JWT ID if available
    expires_at = Column(DateTime(timezone=True), nullable=False)

    # Blacklist details
    blacklisted_at = Column(DateTime(timezone=True), server_default=func.now())
    reason = Column(String(100), nullable=True)  # e.g., "logout", "security_breach"

    # Relationships
    user = relationship("User")

    def __repr__(self):
        return f"<BlacklistedToken(id={self.id}, user_id={self.user_id}, expires_at={self.expires_at})>"