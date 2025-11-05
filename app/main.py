# app/main.py
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from contextlib import asynccontextmanager
from typing import List, Optional

# Import route modules
from app.routes import files, users, metadata, storage_nodes
from app.models.database import init_db, get_db, SessionLocal
from app.models.schemas import HealthCheck, SystemStatus
from app.config.settings import settings
from app.utils.auth import get_current_user
from app.utils.health_check import check_system_health

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dfs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    logger.info("Initializing Distributed File System...")

    try:
        # Initialize database
        init_db()
        logger.info("Database initialized successfully")

        # Initialize storage nodes
        await storage_nodes.initialize_storage_nodes()
        logger.info("Storage nodes initialized")

    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Distributed File System...")
    # Cleanup resources if needed

# Create FastAPI app with lifespan
app = FastAPI(
    title="Distributed File System API",
    description="A RESTful API for managing distributed file storage",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include route modules
app.include_router(
    users.router,
    prefix="/api/v1",
    tags=["Authentication & Users"]
)
app.include_router(
    files.router,
    prefix="/api/v1",
    tags=["File Operations"]
)
app.include_router(
    metadata.router,
    prefix="/api/v1",
    tags=["File Metadata"]
)
app.include_router(
    storage_nodes.router,
    prefix="/api/v1",
    tags=["Storage Nodes Management"]
)

# Root endpoint
@app.get("/", response_model=dict)
async def read_root():
    """Root endpoint with API information"""
    return {
        "message": "Distributed File System API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint for load balancers and monitoring"""
    health_status = await check_system_health()

    return HealthCheck(
        status=health_status["status"],
        database=health_status["database"],
        storage_nodes=health_status["storage_nodes"],
        timestamp=health_status["timestamp"]
    )

# System status endpoint (protected)
@app.get("/status", response_model=SystemStatus)
async def system_status(current_user: dict = Depends(get_current_user)):
    """Get detailed system status (requires authentication)"""
    try:
        db = SessionLocal()
        # Get system statistics
        from sqlalchemy import text
        from models.models import File, User, StorageNode

        # File statistics
        total_files = db.query(File).count()
        total_size = db.execute(text("SELECT COALESCE(SUM(size), 0) FROM files")).scalar()

        # User statistics
        total_users = db.query(User).count()

        # Storage node statistics
        active_nodes = db.query(StorageNode).filter(StorageNode.is_active == True).count()
        total_nodes = db.query(StorageNode).count()

        db.close()

        return SystemStatus(
            total_files=total_files,
            total_size=total_size,
            total_users=total_users,
            active_storage_nodes=active_nodes,
            total_storage_nodes=total_nodes,
            system_health="healthy" if active_nodes > 0 else "degraded"
        )

    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving system status"
        )

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"}
    )

# 404 handler
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": "Endpoint not found"}
    )

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info"
    )