"""Drives a Synapta node through the official UTCP Python SDK (no Synapta code involved).

Run by the Rust test `test_05_official_utcp_sdk` (crates/synapta-gateway/tests). Inputs come
from environment variables set by that test:
  SYNAPTA_MANUAL_TEMPLATE  JSON of the manual call template (from synapta-gateway utcp.rs)
  SYNAPTA_API_TOKEN        the node's local API token
Prints one JSON object with everything the SDK returned; the Rust test asserts on it.
"""

import asyncio
import json
import os

from utcp.utcp_client import UtcpClient


async def main() -> None:
    template = json.loads(os.environ["SYNAPTA_MANUAL_TEMPLATE"])
    token = os.environ["SYNAPTA_API_TOKEN"]
    client = await UtcpClient.create(config={
        # Variables are namespaced by manual name: "synapta" + "_" + SYNAPTA_API_TOKEN.
        "variables": {"synapta_SYNAPTA_API_TOKEN": token},
        "manual_call_templates": [template],
    })

    tools = sorted(t.name for t in await client.config.tool_repository.get_tools())
    auto = await client.call_tool("synapta.math.add", {"body": {"a": 2, "b": 3}})
    explicit = await client.call_tool(
        "synapta.test.execute",
        {"body": {"suite": "smoke", "steps": 1, "step_ms": 0}, "target": "node-c"},
    )
    try:
        await client.call_tool("synapta.math.add", {"body": {"a": 1, "b": 1}, "target": "node-c"})
        refused = None
    except Exception as error:  # the SDK raises on HTTP 4xx with the server's body in the message
        refused = str(error)

    print(json.dumps({"tools": tools, "auto": auto, "explicit": explicit, "refused": refused}))


asyncio.run(main())
