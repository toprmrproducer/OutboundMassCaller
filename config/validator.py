import logging
import os

logger = logging.getLogger("config.validator")

REQUIRED_ENV_VARS = {
    "critical": [
        "DATABASE_URL",
        "LIVEKIT_URL",
        "LIVEKIT_API_KEY",
        "LIVEKIT_API_SECRET",
        "OPENAI_API_KEY",
    ],
    "recommended": [
        "SARVAM_API_KEY",
        "REDIS_URL",
        "SMTP_HOST",
    ],
    "optional": [
        "HUBSPOT_API_KEY",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "PII_MASKING_ENABLED",
        "AUTH_ENABLED",
        "LOG_FORMAT",
        "LOG_LEVEL",
        "DB_POOL_MIN",
        "DB_POOL_MAX",
    ],
}


def validate_env() -> dict:
    result = {
        "ok": True,
        "missing_critical": [],
        "missing_recommended": [],
        "warnings": [],
    }

    for var in REQUIRED_ENV_VARS["critical"]:
        if not os.environ.get(var):
            result["missing_critical"].append(var)
            result["ok"] = False

    for var in REQUIRED_ENV_VARS["recommended"]:
        if not os.environ.get(var):
            result["missing_recommended"].append(var)
            result["warnings"].append(f"{var} not set - related features will be disabled")

    return result


def assert_valid_env():
    result = validate_env()
    for warning in result["warnings"]:
        logger.warning(warning)

    if not result["ok"]:
        for var in result["missing_critical"]:
            logger.critical("Missing critical env var: %s", var)
        raise SystemExit(1)
