# app/routes/storage_nodes.py
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from sqlalchemy.orm import Session
from typing import List
import logging
import requests
import os
import glob
import shutil
import asyncio
from datetime import datetime

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

# In-memory cache for node health
node_health_cache = {}

async def check_node_health(node: StorageNode) -> dict:
    """Check the health of a storage node.

    Behavior:
    - If `settings.USE_HTTP_NODES` is True and the node has a `node_url`, perform
      an HTTP health probe (existing behavior, but executed in a thread).
    - Otherwise, perform filesystem checks against `node.node_path`:
        - Verify path and `chunks/` directory exist
        - Sum chunk file sizes (or use `shutil.disk_usage`) to compute used/available
        - Set health_status based on presence of chunks
    """
    try:
        # Use HTTP probe when configured to use HTTP-based nodes
        if getattr(settings, "USE_HTTP_NODES", False) and node.node_url:
            try:
                def http_probe():
                    return requests.get(f"{node.node_url.rstrip('/')}/health", timeout=5)

                response = await asyncio.to_thread(http_probe)
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
                        "response_time": response.elapsed.total_seconds() if hasattr(response, 'elapsed') else None,
                        "details": f"HTTP {response.status_code}"
                    }
            except requests.exceptions.RequestException as e:
                return {
                    "status": "offline",
                    "response_time": None,
                    "details": str(e)
                }

        # Otherwise, perform filesystem-based health check (local node)
        def fs_probe():
            chunks_dir = os.path.join(node.node_path or "", "chunks")
            info = {"response_time": None}

            if not node.node_path or not os.path.exists(node.node_path):
                info.update({"status": "offline", "details": None})
                return info

            # Ensure chunks directory presence
            chunks_exist = os.path.exists(chunks_dir)

            # Sum sizes of chunk files
            used_bytes = 0
            if chunks_exist:
                for p in glob.glob(os.path.join(chunks_dir, "*.chunk")):
                    try:
                        used_bytes += os.path.getsize(p)
                    except OSError:
                        pass

            # If total_space set, compute available; otherwise attempt disk usage
            try:
                total_space = node.total_space or shutil.disk_usage(node.node_path).total
            except Exception:
                total_space = node.total_space or 0

            available = max(0, (total_space - used_bytes) if total_space else 0)
            # Consider the node healthy if its path exists; an empty chunks directory
            # is not a failure state. Use 'degraded' only for low-availability conditions.
            if not node.node_path or not os.path.exists(node.node_path):
                status = "offline"
            else:
                # If disk space is extremely low, mark degraded; otherwise healthy
                low_space_threshold = 1024 * 1024 * 10  # 10MB safety threshold
                if total_space and available < low_space_threshold:
                    status = "degraded"
                else:
                    status = "healthy"
            details = {"chunks_dir_exists": chunks_exist, "used_bytes": used_bytes, "available_bytes": available}

            info.update({"status": status, "details": details, "used_bytes": used_bytes, "available_bytes": available})
            return info

        fs_info = await asyncio.to_thread(fs_probe)
        return fs_info

    except Exception as e:
        return {"status": "offline", "response_time": None, "details": str(e)}

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
        node.last_heartbeat = datetime.utcnow()

        # If HTTP probe returned simple dict with 'status', use it
        status = health_status.get("status")
        if status:
            node.health_status = status

        # If filesystem probe returned used/available, update node stats
        if "used_bytes" in health_status:
            try:
                node.used_space = int(health_status.get("used_bytes") or 0)
                node.available_space = int(health_status.get("available_bytes") or max(0, (node.total_space or 0) - (node.used_space or 0)))
            except Exception:
                node.used_space = node.used_space or 0
                node.available_space = node.available_space or node.total_space
        else:
            # Keep existing DB values if HTTP probe used
            node.used_space = node.used_space or 0
            node.available_space = node.available_space or max(0, (node.total_space or 0) - (node.used_space or 0))

    db.commit()

@router.on_event("startup")
async def initialize_storage_nodes():
    db = next(get_db())
    try:
        existing_nodes = db.query(StorageNode).count()

        if existing_nodes == 0:
            # Create 10 local nodes for testing
            nodes = [
                {
                    "name": "storage_node_1",
                    "path": "./storage/node1",
                    "priority": 10
                },
                {
                    "name": "storage_node_2",
                    "path": "./storage/node2",
                    "priority": 5
                },
                {
                    "name": "storage_node_3",
                    "path": "./storage/node3",
                    "priority": 5
                },
                {
                    "name": "storage_node_4",
                    "path": "./storage/node4",
                    "priority": 5
                },
                {
                    "name": "storage_node_5",
                    "path": "./storage/node5",
                    "priority": 5
                },
                {
                    "name": "storage_node_6",
                    "path": "./storage/node6",
                    "priority": 10
                },
                {
                    "name": "storage_node_7",
                    "path": "./storage/node7",
                    "priority": 5
                },
                {
                    "name": "storage_node_8",
                    "path": "./storage/node8",
                    "priority": 5
                },
                {
                    "name": "storage_node_9",
                    "path": "./storage/node9",
                    "priority": 5
                },
                {
                    "name": "storage_node_10",
                    "path": "./storage/node10",
                    "priority": 5
                }
            ]

            for i, node_info in enumerate(nodes):
                os.makedirs(node_info["path"], exist_ok=True)

                node = StorageNode(
                    node_name=node_info["name"],
                    node_url=f"http://localhost:{8000 + i}",  # Different "ports"
                    node_path=node_info["path"],
                    total_space=50 * 1024 * 1024 * 1024,  # 50GB each
                    available_space=50 * 1024 * 1024 * 1024,
                    used_space=0,
                    location=f"local_{i+1}",
                    priority=node_info["priority"],
                    is_active=True,
                    health_status="healthy"
                )
                db.add(node)

            db.commit()
            logger.info(f"Created {len(nodes)} local storage nodes")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
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
        # Trigger health check
        task = asyncio.create_task(update_all_nodes_health(db))
        await task

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
        # Trigger health check
        task = asyncio.create_task(update_all_nodes_health(db))
        await task

        nodes = db.query(StorageNode).all()

        healthy_nodes = 0
        total_nodes = len(nodes)

        node_statuses = []
        for node in nodes:
            health_info = node_health_cache.get(node.id, {})
            status = health_info.get("status")

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

        # Trigger health check
        task = asyncio.create_task(update_all_nodes_health(db))
        await task

        # Storage nodes stats
        total_nodes = db.query(StorageNode).count()
        active_nodes = db.query(StorageNode).filter(StorageNode.is_active == True, StorageNode.health_status == "healthy").count()

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

@router.get("/storage/debug")
async def debug_storage_nodes(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Debug endpoint to check storage nodes status"""
    try:
        # Trigger health check
        task = asyncio.create_task(update_all_nodes_health(db))
        await task

        # Check all storage nodes
        all_nodes = db.query(StorageNode).all()

        if not all_nodes:
            return {
                "status": "NO_NODES",
                "message": "No storage nodes exist in database",
                "count": 0
            }

        # Check each node's status
        nodes_info = []
        for node in all_nodes:
            nodes_info.append({
                "id": node.id,
                "name": node.node_name,
                "is_active": node.is_active,
                "health_status": node.health_status,
                "available_space": node.available_space,
                "total_space": node.total_space,
                "url": node.node_url,
                "path": node.node_path,
                "last_heartbeat": node.last_heartbeat
            })

        # Check which nodes would be considered "available"
        from app.utils.node_selector import get_available_storage_nodes
        available_nodes = get_available_storage_nodes(db)

        return {
            "status": "HAS_NODES",
            "total_nodes": len(all_nodes),
            "available_nodes": len(available_nodes),
            "all_nodes": nodes_info,
            "available_node_ids": [n.id for n in available_nodes]
        }

    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e)
        }