# app/routes/users.py
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from datetime import datetime, timedelta
import secrets
import hashlib

from app.models.database import get_db
from app.models.user_models import User, UserSession, UserQuota
from app.models.file_models import FileMetadata
from app.models.schemas import (
    UserCreate, UserResponse, UserLogin, LoginResponse,
    UserProfile, UserQuotaResponse, UserStats
)
from app.utils.auth import create_access_token, verify_password, get_password_hash, get_current_user, security
from jose import JWTError, jwt
from app.config import settings
from app.utils.token_blacklist import add_token_to_blacklist

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/users/register", response_model=UserResponse)
async def register_user(
    user_data: UserCreate,
    db: Session = Depends(get_db)
):
    """Register a new user"""
    try:
        # Check if username already exists
        existing_user = db.query(User).filter(
            (User.username == user_data.username) | (User.email == user_data.email)
        ).first()

        if existing_user:
            raise HTTPException(
                status_code=400,
                detail="Username or email already registered"
            )

        # Generate salt and hash password
        salt = secrets.token_hex(16)
        hashed_password = get_password_hash(user_data.password + salt)

        # Create new user
        new_user = User(
            username=user_data.username,
            email=user_data.email,
            full_name=user_data.full_name,
            hashed_password=hashed_password,
            salt=salt,
            storage_quota=user_data.storage_quota or 1073741824  # Default 1GB
        )

        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        # Create user quota record
        user_quota = UserQuota(
            user_id=new_user.id,
            max_storage_bytes=new_user.storage_quota
        )
        db.add(user_quota)
        db.commit()

        logger.info(f"New user registered: {user_data.username}")

        return UserResponse(
            id=new_user.id,
            username=new_user.username,
            email=new_user.email,
            full_name=new_user.full_name,
            storage_quota=new_user.storage_quota,
            created_at=new_user.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error registering user: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="User registration failed"
        )

@router.post("/users/login", response_model=LoginResponse)
async def login_user(
    login_data: UserLogin,
    db: Session = Depends(get_db)
):
    """Authenticate user and return access token"""
    try:
        # Find user by username or email
        user = db.query(User).filter(
            (User.username == login_data.username) | (User.email == login_data.username)
        ).first()

        if not user or not user.is_active:
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials or inactive account"
            )

        # Check if account is locked
        if user.account_locked_until and user.account_locked_until > datetime.utcnow():
            raise HTTPException(
                status_code=423,
                detail="Account temporarily locked due to failed login attempts"
            )

        # Verify password
        if not verify_password(login_data.password + user.salt, user.hashed_password):
            # Increment failed login attempts
            user.failed_login_attempts += 1

            # Lock account after 5 failed attempts for 30 minutes
            if user.failed_login_attempts >= 5:
                user.account_locked_until = datetime.utcnow() + timedelta(minutes=30)

            db.commit()
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials"
            )

        # Reset failed login attempts on successful login
        user.failed_login_attempts = 0
        user.account_locked_until = None
        user.last_login = datetime.utcnow()
        db.commit()

        # Create access token
        access_token = create_access_token(data={"sub": user.username, "user_id": user.id})

        # Create session record
        session = UserSession(
            user_id=user.id,
            session_token=access_token,
            expires_at=datetime.utcnow() + timedelta(hours=24),
            user_agent=login_data.user_agent,
            ip_address=login_data.ip_address
        )
        db.add(session)
        db.commit()

        logger.info(f"User logged in: {user.username}")

        return LoginResponse(
            access_token=access_token,
            token_type="bearer",
            user=UserResponse(
                id=user.id,
                username=user.username,
                email=user.email,
                full_name=user.full_name,
                storage_quota=user.storage_quota,
                created_at=user.created_at
            )
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error during login: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Login failed"
        )

@router.get("/users/profile", response_model=UserProfile)
async def get_user_profile(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get current user's profile information"""
    #print(f"DEBUG 1: Entering get_user_profile for user: {current_user.username}")

    try:
        #print(f"DEBUG 2: Calculating storage usage...")

        # Calculate current storage usage
        used_storage = db.query(func.coalesce(func.sum(FileMetadata.size), 0)).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).scalar()

        #print(f"DEBUG 3: Used storage: {used_storage}")

        # Get file count
        file_count = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).count()

        #print(f"DEBUG 4: File count: {file_count}")

        # Create the response object
        #print(f"DEBUG 5: Creating UserProfile object...")
        profile_response = UserProfile(
            id=current_user.id,
            username=current_user.username,
            email=current_user.email,
            full_name=current_user.full_name,
            storage_quota=current_user.storage_quota,
            used_storage=used_storage,
            file_count=file_count,
            is_active=current_user.is_active,
            is_verified=current_user.is_verified,
            created_at=current_user.created_at,
            last_login=current_user.last_login
        )

        #print(f"DEBUG 6: Created profile response: {profile_response}")
        #print(f"DEBUG 7: Type of response: {type(profile_response)}")

        return profile_response

    except Exception as e:
        print(f"DEBUG: Exception in get_user_profile: {str(e)}")
        import traceback
        traceback.print_exc()
        logger.error(f"Error getting user profile: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving user profile"
        )

@router.put("/users/profile", response_model=UserResponse)
async def update_user_profile(
    profile_data: UserProfile,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update user profile information"""
    try:
        # Check if email is already taken by another user
        if profile_data.email != current_user.email:
            existing_user = db.query(User).filter(
                User.email == profile_data.email,
                User.id != current_user.id
            ).first()
            if existing_user:
                raise HTTPException(
                    status_code=400,
                    detail="Email already registered"
                )

        # Update user fields
        current_user.email = profile_data.email
        current_user.full_name = profile_data.full_name

        db.commit()
        db.refresh(current_user)

        logger.info(f"User profile updated: {current_user.username}")

        return UserResponse(
            id=current_user.id,
            username=current_user.username,
            email=current_user.email,
            full_name=current_user.full_name,
            storage_quota=current_user.storage_quota,
            created_at=current_user.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating user profile: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Profile update failed"
        )

@router.get("/users/quota", response_model=UserQuotaResponse)
async def get_user_quota(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user storage quota information"""
    try:
        # Calculate current usage
        used_storage = db.query(func.coalesce(func.sum(FileMetadata.size), 0)).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).scalar()

        file_count = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).count()

        usage_percentage = (used_storage / current_user.storage_quota * 100) if current_user.storage_quota > 0 else 0

        return UserQuotaResponse(
            max_storage_bytes=current_user.storage_quota,
            used_storage_bytes=used_storage,
            used_file_count=file_count,
            usage_percentage=round(usage_percentage, 2),
            available_storage_bytes=current_user.storage_quota - used_storage
        )

    except Exception as e:
        logger.error(f"Error getting user quota: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving quota information"
        )

@router.post("/users/logout")
async def logout_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Logout user and invalidate token"""
    try:
        token = credentials.credentials

        # 1. Add token to blacklist
        blacklist_success = add_token_to_blacklist(
            db=db,
            token=token,
            user_id=current_user.id,
            reason="logout"
        )

        # 2. Also invalidate UserSession if exists
        # Find session by token (assuming session_token stores JWT)
        session = db.query(UserSession).filter(
            UserSession.session_token == token,
            UserSession.user_id == current_user.id,
            UserSession.is_active == True
        ).first()

        if session:
            session.is_active = False
            session_invalidated = True
        else:
            session_invalidated = False

        # 3. Also invalidate any associated refresh token
        if session and session.refresh_token:
            try:
                # Blacklist refresh token too
                add_token_to_blacklist(
                    db=db,
                    token=session.refresh_token,
                    user_id=current_user.id,
                    reason="logout_refresh"
                )
            except Exception as e:
                logger.warning(f"Could not blacklist refresh token: {str(e)}")

        db.commit()

        if blacklist_success:
            logger.info(f"User logged out: {current_user.username}")
            return {
                "message": "Successfully logged out",
                "token_invalidated": True,
                "session_invalidated": session_invalidated
            }
        else:
            # Even if blacklisting failed, we should indicate logout
            logger.warning(f"Logout completed but token blacklisting failed for {current_user.username}")
            return {
                "message": "Logged out (token may still be valid temporarily)",
                "token_invalidated": False,
                "session_invalidated": session_invalidated
            }

    except Exception as e:
        db.rollback()
        logger.error(f"Error during logout: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Logout failed"
        )

@router.get("/users/stats", response_model=UserStats)
async def get_user_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user statistics"""
    try:
        # Storage usage
        used_storage = db.query(func.coalesce(func.sum(FileMetadata.size), 0)).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).scalar()

        # File counts by type
        file_counts = db.query(
            FileMetadata.file_extension,
            func.count(FileMetadata.id)
        ).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).group_by(FileMetadata.file_extension).all()

        # Recent activity
        recent_files = db.query(FileMetadata).filter(
            FileMetadata.owner_id == current_user.id,
            FileMetadata.is_deleted == False
        ).order_by(FileMetadata.last_accessed.desc()).limit(5).all()

        return UserStats(
            total_files=sum(count for _, count in file_counts),
            total_storage_used=used_storage,
            file_types={ext: count for ext, count in file_counts},
            storage_quota=current_user.storage_quota,
            quota_usage_percentage=round((used_storage / current_user.storage_quota * 100), 2) if current_user.storage_quota > 0 else 0
        )

    except Exception as e:
        logger.error(f"Error getting user stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving user statistics"
        )