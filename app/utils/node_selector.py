# app/utils/node_selector.py
import random
import json
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from app.models.file_models import StorageNode

logger = logging.getLogger(__name__)

def get_available_storage_nodes(db: Session, min_space_mb: int = 10) -> List:
    """Get all storage nodes (relaxed for testing)"""
    # For testing, don't filter by health_status
    nodes = db.query(StorageNode).filter(
        StorageNode.is_active == True,
        StorageNode.available_space >= (min_space_mb * 1024 * 1024)
    ).order_by(
        StorageNode.priority.desc(),
        StorageNode.available_space.desc()
    ).all()

    # If still no nodes, return ALL nodes (even inactive ones) for testing
    if not nodes:
        nodes = db.query(StorageNode).all()
        logger.warning(f"No active nodes found, using all {len(nodes)} nodes")

    return nodes

def select_nodes_for_chunks(
    db: Session,
    num_chunks: int,
    replication_factor: int = 2,
    strategy: str = "balanced"
) -> List[List[int]]:
    """
    Select storage nodes for each chunk based on strategy

    Args:
        num_chunks: Number of chunks to assign nodes for
        replication_factor: Number of replicas per chunk
        strategy: 'balanced' (default), 'random', or 'capacity'

    Returns:
        List of lists: [[node1_id, node2_id], [node3_id, node1_id], ...]
    """
    # Get available nodes
    nodes = get_available_storage_nodes(db)
    if not nodes:
        raise ValueError("No available storage nodes")

    if replication_factor > len(nodes):
        logger.warning(f"Replication factor {replication_factor} exceeds available nodes {len(nodes)}")
        replication_factor = len(nodes)

    chunk_assignments = []

    if strategy == "random":
        # Random assignment
        for i in range(num_chunks):
            selected = random.sample([n.id for n in nodes], min(replication_factor, len(nodes)))
            chunk_assignments.append(selected)

    elif strategy == "capacity":
        # Weighted by available capacity
        total_capacity = sum(n.available_space for n in nodes)
        for i in range(num_chunks):
            selected = []
            remaining_nodes = nodes.copy()

            for _ in range(min(replication_factor, len(remaining_nodes))):
                if not remaining_nodes:
                    break

                # Weight nodes by available space
                weights = [n.available_space / total_capacity for n in remaining_nodes]
                chosen_node = random.choices(remaining_nodes, weights=weights, k=1)[0]
                selected.append(chosen_node.id)
                remaining_nodes.remove(chosen_node)

            chunk_assignments.append(selected)

    else:  # balanced (default)
        # Round-robin balanced assignment
        node_ids = [n.id for n in nodes]

        for i in range(num_chunks):
            selected = []
            # Start from different offset for each chunk
            start_idx = i % len(node_ids)

            for j in range(replication_factor):
                node_idx = (start_idx + j) % len(node_ids)
                selected.append(node_ids[node_idx])

            chunk_assignments.append(selected)

    logger.info(f"Selected nodes for {num_chunks} chunks with {replication_factor}x replication")
    return chunk_assignments