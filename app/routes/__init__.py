# app/routes/__init__.py
from .files import router as files_router
from .users import router as users_router
from .metadata import router as metadata_router
from .storage_nodes import router as storage_nodes_router

__all__ = [
    "files_router",
    "users_router",
    "metadata_router",
    "storage_nodes_router"
]