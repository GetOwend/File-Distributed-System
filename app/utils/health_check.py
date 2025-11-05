import asyncio
from datetime import datetime
from app.models.database import SessionLocal
from sqlalchemy import text

async def check_system_health():
    """Check the health of system components"""
    health_status = {
        "status": "healthy",
        "database": "healthy",
        "storage_nodes": "healthy",
        "timestamp": datetime.utcnow()
    }

    # Check database connection
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
    except Exception:
        health_status["database"] = "unhealthy"
        health_status["status"] = "degraded"

    # Check storage nodes (simplified)
    # In a real implementation, you'd ping each storage node
    active_nodes = 0  # This would come from your storage node monitoring

    if active_nodes == 0:
        health_status["storage_nodes"] = "unhealthy"
        health_status["status"] = "degraded"

    return health_status