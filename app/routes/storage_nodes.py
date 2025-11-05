# app/routes/storage_nodes.py
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging
import requests
import asyncio
from datetime import datetime, timedelta

from app.models.database import get_db
from app.models.file_models import StorageNode, FileMetadata
from app.models.user_models import User
from app.models.schemas import (
    StorageNodeResponse, StorageNodeCreate, StorageNodeUpdate,
    StorageNodeStatus, NodeHealthResponse, SystemOverview
)
from app.utils.auth import get_current_user
from app.config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter()

# In-memory cache for node health (in production, use Redis)
node_health_cache = {}

async def check_node_health(node: StorageNode) -> dict:
    """Check the health of a storage node"""
    try:
        # Simple health check - try to connect to the node
        health_url = f"{node.node_url}/health"
        response = requests.get(health_url, timeout=5)

        if response.status_code == 200:
            health_data = response.json()
            return {
                "status": "healthy",
                "response_time": response.elapsed.total_seconds(),
                "details": health_data
            }
        else:
            return {
                "status": "unhealthy",
                "response_time": response.elapsed.total_seconds(),
                "details": f"HTTP {response.status_code}"
            }
    except requests.exceptions.RequestException as e:
        return {
            "status": "offline",
            "response_time": None,
            "details": str(e)
        }

async def update_all_nodes_health(db: Session):
    """Update health status for all storage nodes"""
    nodes = db.query(StorageNode).filter(StorageNode.is_active == True).all()

    for node in nodes:
        health_status = await check_node_health(node)
        node_health_cache[node.id] = {
            **health_status,
            "last_checked": datetime.utcnow()
        }

        # Update node status in database
        node.health_status = health_status["status"]
        node.last_heartbeat = datetime.utcnow()

        # Update available space (this would come from the node's health response)
        # For now, we'll simulate it
        if health_status["status"] == "healthy":
            # Simulate space update - in real implementation, get from node
            node.used_space = node.used_space or 0
            node.available_space = max(0, node.total_space - node.used_space)

    db.commit()

@router.on_event("startup")
async def initialize_storage_nodes():
    """Initialize storage nodes on startup"""
    db = next(get_db())
    try:
        # Check if we have any storage nodes, if not create a default local node
        existing_nodes = db.query(StorageNode).count()
        if existing_nodes == 0:
            default_node = StorageNode(
                node_name="local_storage",
                node_url=f"http://{settings.HOST}:{settings.PORT}",
                node_path=settings.STORAGE_PATH,
                total_space=100 * 1024 * 1024 * 1024,  # 100GB
                available_space=100 * 1024 * 1024 * 1024,
                location="local",
                priority=1
            )
            db.add(default_node)
            db.commit()
            logger.info("Default local storage node created")

        # Initial health check
        await update_all_nodes_health(db)

    except Exception as e:
        logger.error(f"Error initializing storage nodes: {str(e)}")
    finally:
        db.close()

@router.get("/storage/nodes", response_model=List[StorageNodeResponse])
async def list_storage_nodes(
    active_only: bool = Query(True, description="Show only active nodes"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all storage nodes in the system"""
    try:
        query = db.query(StorageNode)
        if active_only:
            query = query.filter(StorageNode.is_active == True)

        nodes = query.order_by(StorageNode.priority.desc(), StorageNode.available_space.desc()).all()

        node_responses = []
        for node in nodes:
            health_info = node_health_cache.get(node.id, {})
            node_responses.append(StorageNodeResponse(
                id=node.id,
                node_name=node.node_name,
                node_url=node.node_url,
                node_path=node.node_path,
                total_space=node.total_space,
                used_space=node.used_space,
                available_space=node.available_space,
                is_active=node.is_active,
                health_status=node.health_status,
                location=node.location,
                priority=node.priority,
                last_heartbeat=node.last_heartbeat,
                registered_at=node.registered_at,
                health_details=health_info
            ))

        return node_responses

    except Exception as e:
        logger.error(f"Error listing storage nodes: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving storage nodes")

@router.post("/storage/nodes", response_model=StorageNodeResponse)
async def register_storage_node(
    node_data: StorageNodeCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Register a new storage node"""
    try:
        # Check if node name already exists
        existing_node = db.query(StorageNode).filter(
            StorageNode.node_name == node_data.node_name
        ).first()

        if existing_node:
            raise HTTPException(status_code=400, detail="Node name already exists")

        # Create new storage node
        new_node = StorageNode(
            node_name=node_data.node_name,
            node_url=node_data.node_url,
            node_path=node_data.node_path,
            total_space=node_data.total_space,
            available_space=node_data.total_space,  # Initially all space is available
            used_space=0,
            location=node_data.location,
            priority=node_data.priority or 1
        )

        db.add(new_node)
        db.commit()
        db.refresh(new_node)

        # Check node health
        health_status = await check_node_health(new_node)
        node_health_cache[new_node.id] = {
            **health_status,
            "last_checked": datetime.utcnow()
        }

        logger.info(f"New storage node registered: {node_data.node_name}")

        return StorageNodeResponse(
            id=new_node.id,
            node_name=new_node.node_name,
            node_url=new_node.node_url,
            node_path=new_node.node_path,
            total_space=new_node.total_space,
            used_space=new_node.used_space,
            available_space=new_node.available_space,
            is_active=new_node.is_active,
            health_status=health_status["status"],
            location=new_node.location,
            priority=new_node.priority,
            last_heartbeat=new_node.last_heartbeat,
            registered_at=new_node.registered_at,
            health_details=health_status
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error registering storage node: {str(e)}")
        raise HTTPException(status_code=500, detail="Error registering storage node")

@router.put("/storage/nodes/{node_id}", response_model=StorageNodeResponse)
async def update_storage_node(
    node_id: int,
    node_data: StorageNodeUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update storage node configuration"""
    try:
        node = db.query(StorageNode).filter(StorageNode.id == node_id).first()

        if not node:
            raise HTTPException(status_code=404, detail="Storage node not found")

        # Update allowed fields
        if node_data.node_url is not None:
            node.node_url = node_data.node_url

        if node_data.is_active is not None:
            node.is_active = node_data.is_active

        if node_data.priority is not None:
            node.priority = node_data.priority

        if node_data.total_space is not None:
            # Recalculate available space
            node.total_space = node_data.total_space
            node.available_space = max(0, node_data.total_space - (node.used_space or 0))

        db.commit()
        db.refresh(node)

        # Update health status
        health_status = await check_node_health(node)
        node_health_cache[node.id] = {
            **health_status,
            "last_checked": datetime.utcnow()
        }

        logger.info(f"Storage node updated: {node.node_name}")

        return StorageNodeResponse(
            id=node.id,
            node_name=node.node_name,
            node_url=node.node_url,
            node_path=node.node_path,
            total_space=node.total_space,
            used_space=node.used_space,
            available_space=node.available_space,
            is_active=node.is_active,
            health_status=health_status["status"],
            location=node.location,
            priority=node.priority,
            last_heartbeat=node.last_heartbeat,
            registered_at=node.registered_at,
            health_details=health_status
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating storage node: {str(e)}")
        raise HTTPException(status_code=500, detail="Error updating storage node")

@router.get("/storage/nodes/health", response_model=NodeHealthResponse)
async def get_nodes_health(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get health status of all storage nodes"""
    try:
        # Trigger background health check
        background_tasks.add_task(update_all_nodes_health, db)

        nodes = db.query(StorageNode).all()

        healthy_nodes = 0
        total_nodes = len(nodes)

        node_statuses = []
        for node in nodes:
            health_info = node_health_cache.get(node.id, {})
            status = health_info.get("status", "unknown")

            if status == "healthy":
                healthy_nodes += 1

            node_statuses.append(StorageNodeStatus(
                node_id=node.id,
                node_name=node.node_name,
                status=status,
                last_checked=health_info.get("last_checked"),
                response_time=health_info.get("response_time"),
                details=health_info.get("details")
            ))

        overall_health = "healthy" if healthy_nodes == total_nodes else "degraded"
        if healthy_nodes == 0:
            overall_health = "critical"

        return NodeHealthResponse(
            overall_health=overall_health,
            healthy_nodes=healthy_nodes,
            total_nodes=total_nodes,
            nodes=node_statuses
        )

    except Exception as e:
        logger.error(f"Error getting nodes health: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving nodes health")

@router.get("/storage/overview", response_model=SystemOverview)
async def get_system_overview(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get system overview with storage distribution"""
    try:
        from sqlalchemy import func

        # Storage nodes stats
        total_nodes = db.query(StorageNode).count()
        active_nodes = db.query(StorageNode).filter(StorageNode.is_active == True).count()

        # Total storage capacity
        total_capacity = db.query(func.coalesce(func.sum(StorageNode.total_space), 0)).scalar()
        used_capacity = db.query(func.coalesce(func.sum(StorageNode.used_space), 0)).scalar()
        available_capacity = total_capacity - used_capacity

        # File distribution
        total_files = db.query(FileMetadata).filter(FileMetadata.is_deleted == False).count()
        total_file_size = db.query(func.coalesce(func.sum(FileMetadata.size), 0)).filter(
            FileMetadata.is_deleted == False
        ).scalar()

        # Node usage distribution
        nodes = db.query(StorageNode).all()
        node_usage = []

        for node in nodes:
            # This would be more accurate with proper file-node mapping
            # For now, we'll use the node's own usage metrics
            usage_percentage = (node.used_space / node.total_space * 100) if node.total_space > 0 else 0
            node_usage.append({
                "node_name": node.node_name,
                "used": node.used_space,
                "total": node.total_space,
                "usage_percentage": round(usage_percentage, 2),
                "status": node.health_status
            })

        return SystemOverview(
            total_storage_nodes=total_nodes,
            active_storage_nodes=active_nodes,
            total_capacity=total_capacity,
            used_capacity=used_capacity,
            available_capacity=available_capacity,
            total_files=total_files,
            total_file_size=total_file_size,
            storage_utilization=round((used_capacity / total_capacity * 100), 2) if total_capacity > 0 else 0,
            node_usage=node_usage
        )

    except Exception as e:
        logger.error(f"Error getting system overview: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving system overview")