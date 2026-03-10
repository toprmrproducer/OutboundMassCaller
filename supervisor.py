import os
import uuid

from livekit.api import AccessToken, VideoGrants


def _normalize_mode(mode: str) -> str:
    value = (mode or "").strip().lower()
    if value not in {"listen", "whisper", "barge"}:
        return "listen"
    return value


async def supervisor_join_room(room_id: str, mode: str, supervisor_token: str) -> dict:
    """
    Build a supervisor token for an active call room.

    LISTEN  -> subscribe only, mic disabled
    WHISPER -> publish enabled (UI should route to agent-only track)
    BARGE   -> publish enabled for full participation
    """
    room_name = (room_id or "").strip()
    if not room_name:
        return {}

    normalized_mode = _normalize_mode(mode)
    identity_seed = (supervisor_token or "sup").replace(" ", "_")
    identity = f"supervisor-{identity_seed}-{uuid.uuid4().hex[:6]}"

    can_publish = normalized_mode in {"whisper", "barge"}
    grants = VideoGrants(
        room_join=True,
        room=room_name,
        can_subscribe=True,
        can_publish=can_publish,
        can_publish_data=True,
    )

    token = (
        AccessToken(os.environ["LIVEKIT_API_KEY"], os.environ["LIVEKIT_API_SECRET"])
        .with_identity(identity)
        .with_name("Supervisor")
        .with_grants(grants)
        .with_ttl(3600)
        .to_jwt()
    )
    return {
        "room_url": os.environ.get("LIVEKIT_URL", ""),
        "token": token,
        "room_id": room_name,
        "mode": normalized_mode,
        "identity": identity,
    }
