import asyncio

_shutdown_event = asyncio.Event()


def get_shutdown_event() -> asyncio.Event:
    return _shutdown_event


def request_shutdown() -> None:
    _shutdown_event.set()


def clear_shutdown() -> None:
    _shutdown_event.clear()


async def start_campaign(campaign_id: str):
    from dialer import start_campaign as _start_campaign

    await _start_campaign(campaign_id)


async def stop_campaign(campaign_id: str):
    from dialer import stop_campaign as _stop_campaign

    await _stop_campaign(campaign_id)


async def run_scheduled_callbacks():
    from dialer import run_scheduled_callbacks as _run_scheduled_callbacks

    await _run_scheduled_callbacks()
