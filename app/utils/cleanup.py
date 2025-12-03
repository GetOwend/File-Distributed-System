# app/utils/cleanup.py
import os
import shutil
import logging

logger = logging.getLogger(__name__)

def cleanup_temp_files(temp_dir: str):
    """Clean up temporary files and directories"""
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.debug(f"Cleaned up temp directory: {temp_dir}")
    except Exception as e:
        logger.error(f"Error cleaning up temp directory {temp_dir}: {str(e)}")