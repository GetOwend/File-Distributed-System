# app/utils/chunk_storage.py
import os
import json
import hashlib
import requests
import aiofiles
from typing import Optional, List, Tuple
import concurrent.futures
import time
from app.models.database import SessionLocal
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
            # Send chunk to remote node via HTTP (NOT IMPLEMENTED)
            # DO NOT USE
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

        # If file already exists and matches hash, treat as success (idempotent)
        if os.path.exists(chunk_path):
            try:
                with open(chunk_path, 'rb') as f:
                    existing = f.read()
                if hashlib.sha256(existing).hexdigest() == chunk_hash:
                    logger.debug(f"Chunk already present and valid: {chunk_path}")
                    return True
                else:
                    # Existing file differs; overwrite atomically below
                    logger.warning(f"Existing chunk file differs, will overwrite: {chunk_path}")
            except Exception:
                # If reading fails, attempt to overwrite
                logger.warning(f"Unable to read existing chunk, will overwrite: {chunk_path}")

        # Write to a temporary file then atomically replace
        tmp_path = chunk_path + ".tmp"
        try:
            with open(tmp_path, 'wb') as f:
                f.write(chunk_data)

            # Verify written tmp file
            with open(tmp_path, 'rb') as f:
                stored_data = f.read()
                if len(stored_data) != len(chunk_data):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                    logger.error(f"Chunk size mismatch writing tmp for {chunk_filename}")
                    return False

                verify_hash = hashlib.sha256(stored_data).hexdigest()
                if verify_hash != chunk_hash:
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                    logger.error(f"Chunk hash mismatch writing tmp for {chunk_filename}")
                    return False

            # Atomically move tmp to final path
            os.replace(tmp_path, chunk_path)
            logger.debug(f"Stored chunk locally atomically: {chunk_path} ({len(chunk_data)} bytes)")
            return True
        finally:
            # Ensure tmp cleanup if anything left
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

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


def delete_chunk_locally(node_path: str, file_hash: str, chunk_index: int) -> bool:
    """Delete a chunk file from local node path (best-effort)."""
    try:
        chunks_dir = os.path.join(node_path, "chunks")
        if not os.path.exists(chunks_dir):
            return False

        import glob
        pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}_*.chunk")
        files = glob.glob(pattern)
        if not files:
            pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk_index}.chunk")
            files = glob.glob(pattern)

        removed = 0
        for p in files:
            try:
                os.remove(p)
                removed += 1
            except Exception:
                pass

        return removed > 0
    except Exception:
        return False


def write_chunk_quorum(
    chunk_data: bytes,
    chunk_index: int,
    nodes: List[StorageNode],
    file_hash: str,
    chunk_hash: str,
    w: int = 3,
    timeout: int = 30
) -> List[int]:
    """
    Attempt to write a chunk to multiple nodes concurrently and return list of node IDs that acknowledged.
    If fewer than `w` nodes acknowledge, cleanup any successful writes and return an empty list.
    """
    results: List[Tuple[int, bool]] = []

    def _write(node: StorageNode) -> Tuple[int, bool]:
        try:
            ok = store_chunk_on_node(
                chunk_data=chunk_data,
                chunk_index=chunk_index,
                node=node,
                file_hash=file_hash,
                chunk_hash=chunk_hash
            )
            return (node.id, bool(ok))
        except Exception:
            return (node.id, False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(nodes))) as ex:
        futures = {ex.submit(_write, n): n for n in nodes}

        start = time.time()
        succeeded: List[int] = []
        for fut in concurrent.futures.as_completed(futures, timeout=timeout):
            try:
                node_id, ok = fut.result()
                if ok:
                    succeeded.append(node_id)
            except Exception:
                pass

    if len(succeeded) >= w:
        return succeeded

    # Cleanup partial writes if we couldn't reach quorum
    for node_id in succeeded:
        # find node path
        node = next((n for n in nodes if n.id == node_id), None)
        if node:
            try:
                delete_chunk_locally(node.node_path, file_hash, chunk_index)
            except Exception:
                pass

    return []


def read_chunk_quorum(
    chunk_index: int,
    file_hash: str,
    node_ids: List[int],
    r: int = 3,
    timeout: int = 10
) -> Optional[bytes]:
    """
    Read a chunk concurrently from multiple nodes and return chunk bytes if a majority (r) of nodes
    return the same content (based on SHA256). Returns None if quorum not reached.
    """
    # Worker obtains its own DB session to be thread-safe
    def _read(node_id: int) -> Optional[bytes]:
        db = SessionLocal()
        try:
            return retrieve_chunk_from_node(chunk_index=chunk_index, file_hash=file_hash, node_id=node_id, db=db)
        finally:
            db.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(node_ids))) as ex:
        futures = {ex.submit(_read, nid): nid for nid in node_ids}
        results = []
        start = time.time()
        for fut in concurrent.futures.as_completed(futures, timeout=timeout):
            try:
                data = fut.result()
                if data is not None:
                    results.append(data)
            except Exception:
                pass

    if not results:
        return None

    # Group by hash
    counts = {}
    by_hash = {}
    for data in results:
        h = hashlib.sha256(data).hexdigest()
        counts[h] = counts.get(h, 0) + 1
        by_hash[h] = data

    # Find any hash with >= r votes
    for h, cnt in counts.items():
        if cnt >= r:
            return by_hash[h]

    return None