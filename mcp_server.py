"""
openRMN MCP server — exposes retail media analytics as MCP tools.

Run standalone:
    python mcp_server.py

Or via stdio (for Claude Desktop):
    The MCP client launches this process and communicates via stdio.
"""

import asyncio
import json
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from connectors import fetch_all
from agent import (
    compute_kpis,
    detect_anomalies,
    neutrality_audit,
    trust_score,
    double_counting_audit,
    simulate_harmonization,
    NETWORK_METHODOLOGIES,
)

app = Server("openrmn")

# ── Tools ──


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_kpis",
            description=(
                "Get unified retail media KPIs across all networks "
                "(Amazon Ads, Criteo Retail Media, Unlimitail). "
                "Returns total spend, total attributed sales, unified ROAS, "
                "and per-network breakdown. Use this for high-level "
                "performance questions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to look back (default 14)",
                        "default": 14,
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["mock", "real", "auto"],
                        "description": "Data source mode (default 'mock' for demo)",
                        "default": "mock",
                    },
                },
            },
        ),
        Tool(
            name="get_anomalies",
            description=(
                "Detect cross-network anomalies: products with diverging "
                "ROAS between networks (potential cannibalization), and "
                "underperforming campaigns (>5k spend, <1.5 ROAS). "
                "Use this when the advertiser asks 'what's wrong?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "default": 14},
                    "mode": {"type": "string", "default": "mock"},
                },
            },
        ),
        Tool(
            name="get_trust_score",
            description=(
                "Compute a Trust Score (0-100, A+/B/C grade) per retail "
                "media network based on 4 weighted components: internal "
                "consistency, cross-network convergence, methodology "
                "transparency, data freshness. Use this to answer "
                "'which network's numbers can I trust more?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "default": 14},
                    "mode": {"type": "string", "default": "mock"},
                },
            },
        ),
        Tool(
            name="get_double_counting_audit",
            description=(
                "Estimate over-attribution across networks. Each network "
                "claims its own sales; the sum is typically 30-50% higher "
                "than reality. Returns total declared, estimated real, "
                "overlap amount and percentage. Use this when the advertiser "
                "asks 'how much of my reported sales is actually real?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "default": 14},
                    "mode": {"type": "string", "default": "mock"},
                },
            },
        ),
        Tool(
            name="simulate_harmonization",
            description=(
                "Simulate what KPIs would look like if all networks used "
                "the same attribution window and conversion type. Each "
                "network currently uses different windows (Amazon 7d, "
                "Criteo 30d, Unlimitail 14d). Use this when the advertiser "
                "asks 'what if everyone followed the same rules?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "target_window_days": {
                        "type": "integer",
                        "description": "Target attribution window (7, 14, or 30)",
                        "default": 14,
                    },
                    "target_type": {
                        "type": "string",
                        "enum": ["last-click", "last-click + assisted"],
                        "description": "Target attribution type (default 'last-click')",
                        "default": "last-click",
                    },
                    "days": {"type": "integer", "default": 14},
                    "mode": {"type": "string", "default": "mock"},
                },
            },
        ),
        Tool(
            name="get_neutrality_audit",
            description=(
                "Compare attribution shares per product across networks. "
                "Flags products where one network claims more than 70% of "
                "attributed sales — a strong signal of walled-garden "
                "over-attribution. Use this for neutrality assessment."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "default": 14},
                    "mode": {"type": "string", "default": "mock"},
                },
            },
        ),
        Tool(
            name="get_methodology_comparison",
            description=(
                "Return a side-by-side comparison of each network's "
                "attribution methodology (window, conversion type, "
                "view-through handling, MRC compliance, documentation URL)."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    days = arguments.get("days", 14)
    mode = arguments.get("mode", "mock")

    if name == "get_kpis":
        df = fetch_all(days=days, mode=mode)
        result = compute_kpis(df)
    elif name == "get_anomalies":
        df = fetch_all(days=days, mode=mode)
        result = {"anomalies": detect_anomalies(df)}
    elif name == "get_trust_score":
        df = fetch_all(days=days, mode=mode)
        result = trust_score(df)
    elif name == "get_double_counting_audit":
        df = fetch_all(days=days, mode=mode)
        result = double_counting_audit(df)
    elif name == "simulate_harmonization":
        df = fetch_all(days=days, mode=mode)
        result = simulate_harmonization(
            df,
            target_window_days=arguments.get("target_window_days", 14),
            target_type=arguments.get("target_type", "last-click"),
        )
    elif name == "get_neutrality_audit":
        df = fetch_all(days=days, mode=mode)
        result = neutrality_audit(df)
    elif name == "get_methodology_comparison":
        result = NETWORK_METHODOLOGIES
    else:
        result = {"error": f"Unknown tool: {name}"}

    return [
        TextContent(
            type="text",
            text=json.dumps(result, indent=2, default=str, ensure_ascii=False),
        )
    ]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
