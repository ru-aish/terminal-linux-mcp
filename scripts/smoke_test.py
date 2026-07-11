#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client


async def inspect_server(url: str) -> None:
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            response = await session.list_tools()

    names = [tool.name for tool in response.tools]
    print(f"Connected to {url}")
    print(f"Discovered {len(names)} tools:")
    for name in names:
        print(f"- {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Connect to a Terminal MCP endpoint and list its tools.")
    parser.add_argument("url", nargs="?", default="http://127.0.0.1:8000/mcp")
    args = parser.parse_args()
    asyncio.run(inspect_server(args.url))


if __name__ == "__main__":
    main()
