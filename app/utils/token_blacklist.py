# app/utils/token_blacklist.py
import hashlib
from datetime import datetime
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from app.config import settings
from app.models.user_models import BlacklistedToken
import logging

logger = logging.getLogger(__name__)

def get_token_hash(token: str) -> str:
    """Generate SHA256 hash of token for storage and indexing"""
    return hashlib.sha256(token.encode()).hexdigest()

def add_token_to_blacklist(
    db: Session,
    token: str,
    user_id: int,
    reason: str = "logout"
) -> bool:
    """
    Add a token to the blacklist
    Returns True if successful, False otherwise
    """
    try:
        # Decode token to get expiration and jti
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=["HS256"],
            options={"verify_exp": False}  # Don't verify expiration since we're blacklisting it
        )

        # Get expiration time
        expires_at = datetime.fromtimestamp(payload["exp"])

        # Get JWT ID if exists
        jti = payload.get("jti")

        # Create blacklisted token record
        blacklisted_token = BlacklistedToken(
            token=token,
            token_hash=get_token_hash(token),
            user_id=user_id,
            jti=jti,
            expires_at=expires_at,
            reason=reason
        )

        db.add(blacklisted_token)
        db.commit()

        logger.info(f"Token blacklisted for user_id: {user_id}, reason: {reason}")
        return True

    except JWTError as e:
        logger.error(f"Invalid token format during blacklisting: {str(e)}")
        return False
    except Exception as e:
        db.rollback()
        logger.error(f"Error blacklisting token: {str(e)}")
        return False

def is_token_blacklisted(db: Session, token: str) -> bool:
    """
    Check if token is blacklisted
    Returns True if blacklisted, False if not found or valid
    """
    try:
        # First, try to find by hash (faster lookup)
        token_hash = get_token_hash(token)
        blacklisted = db.query(BlacklistedToken).filter(
            BlacklistedToken.token_hash == token_hash
        ).first()

        # If not found by hash, try by full token (fallback)
        if not blacklisted:
            blacklisted = db.query(BlacklistedToken).filter(
                BlacklistedToken.token == token
            ).first()

        if blacklisted:
            # Optional: Clean up expired tokens when we find one
            if datetime.utcnow() > blacklisted.expires_at:
                db.delete(blacklisted)
                db.commit()
                return False  # Token expired, not blacklisted
            return True

        return False

    except Exception as e:
        logger.error(f"Error checking token blacklist: {str(e)}")
        return False  # On error, assume not blacklisted to avoid blocking valid users

def blacklist_all_user_tokens(db: Session, user_id: int, reason: str = "security") -> int:
    """
    Blacklist all tokens for a specific user (e.g., on password change)
    Returns number of tokens blacklisted
    """
    # Note: This is a heavy operation, use sparingly
    # In practice, you might want to implement a different strategy
    pass  # Implement if needed

def cleanup_expired_tokens(db: Session) -> int:
    """
    Clean up expired blacklisted tokens from database
    Returns number of tokens removed
    """
    try:
        now = datetime.utcnow()
        result = db.query(BlacklistedToken).filter(
            BlacklistedToken.expires_at < now
        ).delete()
        db.commit()

        logger.info(f"Cleaned up {result} expired blacklisted tokens")
        return result

    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning up expired tokens: {str(e)}")
        return 0