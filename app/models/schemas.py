from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Optional

class HealthCheck(BaseModel):
    status: str
    database: str
    storage_nodes: str
    timestamp: datetime

class SystemStatus(BaseModel):
    total_files: int
    total_size: int
    total_users: int
    active_storage_nodes: int
    total_storage_nodes: int
    system_health: str

class ErrorResponse(BaseModel):
    detail: str

# File Schemas
class FileResponse(BaseModel):
    id: int
    filename: str
    size: int
    file_path: str
    created_at: datetime

class FileUploadResponse(BaseModel):
    message: str
    file_id: int
    filename: str
    size: int

class FileListResponse(BaseModel):
    files: List[FileResponse]
    total_count: int

class FileDeleteResponse(BaseModel):
    message: str
    file_id: int
    filename: str

# User Schemas
class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    full_name: Optional[str] = None
    storage_quota: Optional[int] = None

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: Optional[str]
    storage_quota: int
    created_at: datetime

class UserLogin(BaseModel):
    username: str
    password: str
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserResponse

class UserProfile(BaseModel):
    id: int
    username: str
    email: str
    full_name: Optional[str]
    storage_quota: int
    used_storage: int
    file_count: int
    is_active: bool
    is_verified: bool
    created_at: datetime
    last_login: Optional[datetime]

class UserQuotaResponse(BaseModel):
    max_storage_bytes: int
    used_storage_bytes: int
    used_file_count: int
    usage_percentage: float
    available_storage_bytes: int

class UserStats(BaseModel):
    total_files: int
    total_storage_used: int
    file_types: Dict[str, int]
    storage_quota: int
    quota_usage_percentage: float

class FileDownloadResponse(BaseModel):
    file_id: int
    filename: str
    download_url: str
    size: int
    message: Optional[str] = "File ready for download"

# Metadata Schemas
class FileMetadataDetail(BaseModel):
    id: int
    filename: str
    storage_filename: str
    size: int
    file_path: str
    file_hash: str
    content_type: str
    file_extension: str
    replication_factor: int
    storage_nodes: List[str]
    is_compressed: bool
    is_public: bool
    permissions: str
    chunks: List[dict]
    created_at: datetime
    updated_at: Optional[datetime]
    last_accessed: Optional[datetime]

class FileUpdateRequest(BaseModel):
    is_public: Optional[bool] = None
    permissions: Optional[str] = None

class FileSearchResponse(BaseModel):
    files: List[FileResponse]
    total_count: int
    query: str
    filters: dict

class MetadataResponse(BaseModel):
    total_files: int
    total_size: int
    files_by_type: Dict[str, dict]
    storage_distribution: Dict[str, float]
    recent_files: List[dict]

# Storage Node Schemas
class StorageNodeResponse(BaseModel):
    id: int
    node_name: str
    node_url: str
    node_path: str
    total_space: int
    used_space: Optional[int]
    available_space: int
    is_active: bool
    health_status: str
    location: Optional[str]
    priority: int
    last_heartbeat: Optional[datetime]
    registered_at: datetime
    health_details: Optional[dict] = None

class StorageNodeCreate(BaseModel):
    node_name: str
    node_url: str
    node_path: str
    total_space: int
    location: Optional[str] = None
    priority: Optional[int] = 1

class StorageNodeUpdate(BaseModel):
    node_url: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    total_space: Optional[int] = None

class StorageNodeStatus(BaseModel):
    node_id: int
    node_name: str
    status: str
    last_checked: Optional[datetime]
    response_time: Optional[float]
    details: Optional[str]

class NodeHealthResponse(BaseModel):
    overall_health: str
    healthy_nodes: int
    total_nodes: int
    nodes: List[StorageNodeStatus]

class SystemOverview(BaseModel):
    total_storage_nodes: int
    active_storage_nodes: int
    total_capacity: int
    used_capacity: int
    available_capacity: int
    total_files: int
    total_file_size: int
    storage_utilization: float
    node_usage: List[dict]