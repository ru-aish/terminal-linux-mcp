#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def _list_tools(url: str, http_client: httpx.AsyncClient | None = None):
    async with streamable_http_client(url, http_client=http_client) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await session.list_tools()


async def inspect_server(url: str, token: str | None) -> None:
    if token:
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"}) as client:
            response = await _list_tools(url, client)
    else:
        response = await _list_tools(url)

    names = [tool.name for tool in response.tools]
    print(f"Connected to {url}")
    print(f"Discovered {len(names)} tools:")
    for name in names:
        print(f"- {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Connect to a Terminal MCP endpoint and list its tools.")
    parser.add_argument("url", nargs="?", default="http://127.0.0.1:8011/mcp")
    parser.add_argument(
        "--token",
        default=os.environ.get("MCP_BEARER_TOKEN"),
        help="Bearer token. Defaults to MCP_BEARER_TOKEN.",
    )
    args = parser.parse_args()
    asyncio.run(inspect_server(args.url, args.token))


if __name__ == "__main__":
    main()
