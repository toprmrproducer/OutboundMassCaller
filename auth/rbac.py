import hashlib
import os
import secrets

ROLE_PERMISSIONS = {
    "superadmin": ["*"],
    "admin": ["campaigns:*", "leads:*", "agents:*", "analytics:*", "settings:*"],
    "manager": ["campaigns:read", "campaigns:create", "campaigns:update", "leads:*", "analytics:read"],
    "agent_supervisor": ["campaigns:read", "leads:read", "supervisor:*", "analytics:read"],
    "viewer": ["campaigns:read", "leads:read", "analytics:read"],
}


def has_permission(role: str, permission: str) -> bool:
    grants = ROLE_PERMISSIONS.get(role or "viewer", [])
    if "*" in grants:
        return True
    if permission in grants:
        return True
    namespace = permission.split(":", 1)[0]
    if f"{namespace}:*" in grants:
        return True
    return False


def generate_token(user_id: str, expiry_hours: int = 24) -> str:
    _ = user_id
    _ = expiry_hours
    return secrets.token_urlsafe(32)


def get_password_hash(password: str) -> str:
    salt = os.urandom(32)
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return salt.hex() + ":" + key.hex()


def verify_password(password: str, hashed: str) -> bool:
    salt_hex, key_hex = hashed.split(":")
    salt = bytes.fromhex(salt_hex)
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return key.hex() == key_hex
