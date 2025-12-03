# app/utils/file_chunker.py
import os
import hashlib
import aiofiles
from typing import List, Tuple
import logging
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def split_file_into_chunks(file_path: str, chunk_size_mb: int = 5) -> List[bytes]:
    """Split file into chunks of specified size"""
    chunks = []
    chunk_size = chunk_size_mb * 1024 * 1024

    try:
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        logger.info(f"Split {file_path} into {len(chunks)} chunks of max {chunk_size_mb}MB each")
        return chunks

    except Exception as e:
        logger.error(f"Error splitting file {file_path}: {str(e)}")
        raise

async def async_split_file_into_chunks(file_path: str, chunk_size_mb: int = 5) -> List[bytes]:
    """Async version of file chunking"""
    chunks = []
    chunk_size = chunk_size_mb * 1024 * 1024

    try:
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        return chunks

    except Exception as e:
        logger.error(f"Error splitting file {file_path}: {str(e)}")
        raise

def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA256 hash of a file"""
    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()