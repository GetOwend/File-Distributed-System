# app/utils/chunk_storage.py
import os
import json
import hashlib
import requests
import aiofiles
from typing import Optional
import logging
from sqlalchemy.orm import Session
from app.models.file_models import StorageNode

logger = logging.getLogger(__name__)

def store_chunk_on_node(
    chunk_data: bytes,
    chunk_index: int,
    node,
    file_hash: str,
    chunk_hash: str,
    storage_method: str = "local"
) -> bool:
    """
    Store a chunk on a specific storage node

    Args:
        chunk_data: The chunk bytes
        chunk_index: Index of this chunk
        node: StorageNode object
        file_hash: Hash of the complete file
        chunk_hash: Hash of this specific chunk
        storage_method: 'local' or 'http'

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if storage_method == "http" and node.node_url and "localhost" not in node.node_url:
            # Send chunk to remote node via HTTP
            return store_chunk_via_http(
                chunk_data=chunk_data,
                chunk_index=chunk_index,
                node_url=node.node_url,
                file_hash=file_hash,
                chunk_hash=chunk_hash
            )
        else:
            # Store locally (for localhost/testing)
            return store_chunk_locally(
                chunk_data=chunk_data,
                chunk_index=chunk_index,
                node_path=node.node_path,
                file_hash=file_hash,
                chunk_hash=chunk_hash
            )

    except Exception as e:
        logger.error(f"Failed to store chunk {chunk_index} on node {node.node_name}: {str(e)}")
        return False

def store_chunk_locally(
    chunk_data: bytes,
    chunk_index: int,
    node_path: str,
    file_hash: str,
    chunk_hash: str
) -> bool:
    """Store chunk in local filesystem"""
    try:
        # Create chunks directory if it doesn't exist
        chunks_dir = os.path.join(node_path, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)

        # Create filename: {file_hash}_{chunk_index}_{chunk_hash[:8]}.chunk
        chunk_filename = f"{file_hash}_{chunk_index}_{chunk_hash[:8]}.chunk"
        chunk_path = os.path.join(chunks_dir, chunk_filename)

        # Save chunk
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)

        # Verify the chunk was written correctly
        with open(chunk_path, 'rb') as f:
            stored_data = f.read()
            if len(stored_data) != len(chunk_data):
                os.remove(chunk_path)
                logger.error(f"Chunk size mismatch for {chunk_filename}")
                return False

            # Optional: verify hash
            verify_hash = hashlib.sha256(stored_data).hexdigest()
            if verify_hash != chunk_hash:
                os.remove(chunk_path)
                logger.error(f"Chunk hash mismatch for {chunk_filename}")
                return False

        logger.debug(f"Stored chunk locally: {chunk_path} ({len(chunk_data)} bytes)")
        return True

    except Exception as e:
        logger.error(f"Error storing chunk locally: {str(e)}")
        return False

def store_chunk_via_http(
    chunk_data: bytes,
    chunk_index: int,
    node_url: str,
    file_hash: str,
    chunk_hash: str,
    timeout: int = 30
) -> bool:
    """Send chunk to remote storage node via HTTP"""
    try:
        upload_url = f"{node_url.rstrip('/')}/api/storage/chunk/upload"

        files = {
            'chunk': (f'{file_hash}_{chunk_index}.chunk', chunk_data, 'application/octet-stream')
        }

        data = {
            'file_hash': file_hash,
            'chunk_index': chunk_index,
            'chunk_hash': chunk_hash
        }

        response = requests.post(
            upload_url,
            files=files,
            data=data,
            timeout=timeout
        )

        if response.status_code == 200:
            logger.debug(f"Successfully sent chunk {chunk_index} to {node_url}")
            return True
        else:
            logger.error(f"Failed to send chunk to {node_url}: HTTP {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error sending chunk to {node_url}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error in HTTP chunk storage: {str(e)}")
        return False

def retrieve_chunk_from_node(
    chunk_index: int,
    file_hash: str,
    node_id: int,
    db: Session
) -> Optional[bytes]:
    """
    Retrieve a chunk from a specific storage node

    Args:
        chunk_index: Index of the chunk
        file_hash: Hash of the complete file
        node_id: ID of the storage node
        db: Database session

    Returns:
        bytes: Chunk data, or None if not found
    """
    try:
        storage_node = db.query(StorageNode).filter(StorageNode.id == node_id).first()
        if not storage_node:
            logger.error(f"Storage node {node_id} not found")
            return None

        # For local nodes, search for chunk file
        chunks_dir = os.path.join(storage_node.node_path, "chunks")

        if not os.path.exists(chunks_dir):
            logger.error(f"Chunks directory not found: {chunks_dir}")
            return None

        # Look for chunk file - pattern: {file_hash}_{chunk_index}_*.chunk
        import glob
        chunk_pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}_*.chunk")
        chunk_files = glob.glob(chunk_pattern)

        if not chunk_files:
            # Try alternative pattern without wildcard hash
            chunk_pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}.chunk")
            chunk_files = glob.glob(chunk_pattern)

        if not chunk_files:
            logger.error(f"No chunk file found for pattern: {chunk_pattern}")
            return None

        # Read the first matching chunk file
        chunk_path = chunk_files[0]

        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()

        logger.debug(f"Retrieved chunk {chunk_index} from {storage_node.node_name}")
        return chunk_data

    except Exception as e:
        logger.error(f"Error retrieving chunk {chunk_index} from node {node_id}: {str(e)}")
        return None