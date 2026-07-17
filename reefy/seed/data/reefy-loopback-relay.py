#!/usr/bin/env python3
"""Expose a container-loopback TCP service through its published port.

Hermes intentionally permits an unauthenticated dashboard only on loopback.
This relay listens on the container port published by Reefy and forwards raw
TCP streams to the loopback-only dashboard. Raw forwarding preserves HTTP,
WebSocket, SSE, and any future protocols without application-level rewriting.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from dataclasses import dataclass
from functools import partial


LOG = logging.getLogger("reefy-loopback-relay")
BUFFER_SIZE = 64 * 1024


def _env_port(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535, got {port}")
    return port


@dataclass(frozen=True)
class RelayConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 9119
    target_host: str = "127.0.0.1"
    target_port: int = 9120
    connect_timeout: float = 5.0

    @classmethod
    def from_env(cls) -> "RelayConfig":
        timeout_raw = os.getenv("REEFY_RELAY_CONNECT_TIMEOUT", "5")
        try:
            timeout = float(timeout_raw)
        except ValueError as exc:
            raise ValueError(
                "REEFY_RELAY_CONNECT_TIMEOUT must be a number, "
                f"got {timeout_raw!r}"
            ) from exc
        if timeout <= 0:
            raise ValueError("REEFY_RELAY_CONNECT_TIMEOUT must be positive")
        return cls(
            listen_host=os.getenv("REEFY_RELAY_LISTEN_HOST", "0.0.0.0"),
            listen_port=_env_port("REEFY_RELAY_LISTEN_PORT", 9119),
            target_host=os.getenv("REEFY_RELAY_TARGET_HOST", "127.0.0.1"),
            target_port=_env_port("REEFY_RELAY_TARGET_PORT", 9120),
            connect_timeout=timeout,
        )


async def _close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    writer.close()
    with contextlib.suppress(ConnectionError, BrokenPipeError):
        await writer.wait_closed()


async def _copy_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    while data := await reader.read(BUFFER_SIZE):
        writer.write(data)
        await writer.drain()
    with contextlib.suppress(AttributeError, ConnectionError, OSError):
        writer.write_eof()


async def relay_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    config: RelayConfig,
) -> None:
    upstream_writer: asyncio.StreamWriter | None = None
    peer = client_writer.get_extra_info("peername")
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(config.target_host, config.target_port),
            timeout=config.connect_timeout,
        )
        await asyncio.gather(
            _copy_stream(client_reader, upstream_writer),
            _copy_stream(upstream_reader, client_writer),
        )
    except asyncio.CancelledError:
        raise
    except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
        LOG.warning(
            "relay connection failed peer=%r target=%s:%d error=%s",
            peer,
            config.target_host,
            config.target_port,
            exc,
        )
    finally:
        await _close_writer(upstream_writer)
        await _close_writer(client_writer)


async def start_relay(config: RelayConfig) -> asyncio.AbstractServer:
    return await asyncio.start_server(
        partial(relay_connection, config=config),
        config.listen_host,
        config.listen_port,
    )


async def run(config: RelayConfig) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    server = await start_relay(config)
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    LOG.info(
        "listening on %s and forwarding to %s:%d",
        addresses,
        config.target_host,
        config.target_port,
    )
    async with server:
        await stop.wait()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("REEFY_RELAY_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        config = RelayConfig.from_env()
    except ValueError as exc:
        raise SystemExit(f"invalid relay configuration: {exc}") from exc
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
