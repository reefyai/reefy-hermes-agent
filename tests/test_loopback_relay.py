import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path


RELAY_PATH = (
    Path(__file__).parents[1]
    / "reefy"
    / "seed"
    / "data"
    / "reefy-loopback-relay.py"
)
SPEC = importlib.util.spec_from_file_location("reefy_loopback_relay", RELAY_PATH)
relay = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = relay
SPEC.loader.exec_module(relay)


class LoopbackRelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async def echo(reader, writer):
            try:
                while data := await reader.read(65536):
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        self.upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
        target_port = self.upstream.sockets[0].getsockname()[1]
        self.relay = await relay.start_relay(
            relay.RelayConfig(
                listen_host="127.0.0.1",
                listen_port=0,
                target_host="127.0.0.1",
                target_port=target_port,
            )
        )

    async def asyncTearDown(self):
        self.relay.close()
        await self.relay.wait_closed()
        self.upstream.close()
        await self.upstream.wait_closed()

    async def test_forwards_binary_data_in_both_directions(self):
        relay_port = self.relay.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", relay_port)
        payload = b"GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n\x00\xff"

        writer.write(payload)
        await writer.drain()

        self.assertEqual(await reader.readexactly(len(payload)), payload)
        writer.close()
        await writer.wait_closed()

    async def test_closes_client_when_target_is_unavailable(self):
        unavailable = await relay.start_relay(
            relay.RelayConfig(
                listen_host="127.0.0.1",
                listen_port=0,
                target_host="127.0.0.1",
                target_port=1,
                connect_timeout=0.1,
            )
        )
        try:
            port = unavailable.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            self.assertEqual(await asyncio.wait_for(reader.read(), timeout=1), b"")
            writer.close()
            await writer.wait_closed()
        finally:
            unavailable.close()
            await unavailable.wait_closed()


if __name__ == "__main__":
    unittest.main()
