import os
import json
import logging
from typing import Optional

from app.models.database import SessionLocal
from app.models.file_models import FileChunk, StorageNode, FileMetadata
from app.utils.chunk_storage import retrieve_chunk_from_node, store_chunk_on_node

logger = logging.getLogger(__name__)


def repair_file_chunks(file_hash: str) -> dict:
    """Repair chunks for a file by recreating missing node directories and copying
    existing replicas to those nodes.

    Behavior:
    - For each chunk, checks `stored_on_nodes` in metadata.
    - If a claimed node is missing the chunk file or its directory, tries to recreate
      the node's `chunks` directory and write the chunk file there by copying from
      a readable replica.
    - Updates `stored_on_nodes`, `backup_node_ids`, `primary_node_id` as needed and
      updates node usage stats.

    Returns a summary dict with counts and any errors.
    """
    db = SessionLocal()
    summary = {"repaired": 0, "skipped_no_source": 0, "errors": []}

    try:
        # Lock the file metadata row to avoid concurrent repairs/uploads
        file_meta = db.query(FileMetadata).filter(FileMetadata.file_hash == file_hash).with_for_update().first()
        if not file_meta:
            msg = f"No metadata for file hash {file_hash[:8]}"
            logger.warning(msg)
            return {"error": msg}

        chunks = db.query(FileChunk).filter(FileChunk.file_hash == file_hash).all()
        if not chunks:
            msg = f"No chunks found for file hash {file_hash[:8]}"
            logger.info(msg)
            return {"error": msg}

        for chunk in chunks:
            try:
                # Re-fetch chunk with FOR UPDATE to lock the row before modifications
                chunk_row = db.query(FileChunk).filter(
                    FileChunk.file_hash == chunk.file_hash,
                    FileChunk.chunk_index == chunk.chunk_index
                ).with_for_update().first()

                if not chunk_row:
                    logger.warning(f"Chunk row disappeared for {chunk.chunk_index} of {file_hash[:8]}")
                    continue

                stored_on = json.loads(chunk_row.stored_on_nodes) if chunk_row.stored_on_nodes else []
                # Build map of node_id -> StorageNode
                node_map = {}
                missing_node_objs = []
                for nid in stored_on:
                    node = db.query(StorageNode).filter(StorageNode.id == nid).first()
                    if not node:
                        logger.warning(f"StorageNode entry {nid} missing in DB for chunk {chunk.chunk_index}")
                        continue

                    # Check for chunk file existence
                    chunks_dir = os.path.join(node.node_path, "chunks")
                    if not os.path.exists(chunks_dir):
                        # directory missing => considered missing
                        missing_node_objs.append(node)
                        node_map[nid] = node
                        continue

                    import glob
                    pattern = os.path.join(chunks_dir, f"{file_hash}_{chunk.chunk_index}_*.chunk")
                    matches = glob.glob(pattern)
                    if not matches:
                        # chunk file not present
                        missing_node_objs.append(node)
                        node_map[nid] = node
                    else:
                        node_map[nid] = node

                # Find a readable source from the remaining stored_on entries
                source_data = None
                for nid, node in node_map.items():
                    # Skip nodes we flagged as missing
                    if any(n.id == nid for n in missing_node_objs):
                        continue
                    try:
                        data = retrieve_chunk_from_node(
                            chunk_index=chunk.chunk_index,
                            file_hash=file_hash,
                            node_id=nid,
                            db=db
                        )
                    except Exception:
                        data = None
                    if data:
                        source_data = data
                        break

                if source_data is None:
                    # No source available to copy from
                    logger.error(f"DATA LOSS: No readable replica available for chunk {chunk.chunk_index} of {file_hash[:8]}")
                    summary["skipped_no_source"] += 1
                    continue

                # For each missing node, recreate dirs and store the chunk
                for node in missing_node_objs:
                    try:
                        # Ensure node directories exist
                        os.makedirs(node.node_path, exist_ok=True)
                        chunks_dir = os.path.join(node.node_path, "chunks")
                        os.makedirs(chunks_dir, exist_ok=True)

                        # Store chunk on node
                        success = store_chunk_on_node(
                            chunk_data=source_data,
                            chunk_index=chunk.chunk_index,
                            node=node,
                            file_hash=file_hash,
                            chunk_hash=chunk.chunk_hash
                        )

                        if success:
                            # update stored_on list
                            if node.id not in stored_on:
                                stored_on.append(node.id)

                            # update node usage with row lock
                            node_row = db.query(StorageNode).filter(StorageNode.id == node.id).with_for_update().first()
                            if node_row:
                                node_row.used_space = (node_row.used_space or 0) + chunk.size
                                node_row.available_space = max(0, node_row.total_space - node_row.used_space)
                                db.add(node_row)

                            logger.info(f"Repaired chunk {chunk.chunk_index} on node {node.id} ({node.node_path})")
                            summary["repaired"] += 1
                        else:
                            logger.error(f"Failed to store chunk {chunk.chunk_index} on recreated node {node.id}")
                            summary["errors"].append(f"store_failed: node={node.id},chunk={chunk.chunk_index}")

                    except Exception as e:
                        logger.error(f"Error repairing chunk {chunk.chunk_index} on node {getattr(node, 'id', None)}: {str(e)}")
                        summary["errors"].append(str(e))

                # Update chunk metadata after attempts
                # Ensure primary remains valid; if not, pick first stored_on
                stored_on = list(dict.fromkeys(stored_on))  # dedupe while preserving order
                chunk_row.stored_on_nodes = json.dumps(stored_on)
                chunk_row.backup_node_ids = json.dumps(stored_on[1:]) if len(stored_on) > 1 else None
                if chunk_row.primary_node_id not in stored_on:
                    chunk_row.primary_node_id = stored_on[0] if stored_on else None
                chunk_row.is_stored = len(stored_on) > 0
                db.add(chunk_row)
                db.commit()

            except Exception as e:
                logger.error(f"Error processing chunk {chunk.chunk_index} for repair: {str(e)}")
                summary["errors"].append(str(e))

        return summary

    finally:
        db.close()


def repair_file_by_id(file_id: int) -> dict:
    """Convenience wrapper: look up file_hash by file_id and run repair."""
    db = SessionLocal()
    try:
        fm = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
        if not fm:
            msg = f"File id {file_id} not found"
            logger.warning(msg)
            return {"error": msg}
        return repair_file_chunks(fm.file_hash)
    finally:
        db.close()
