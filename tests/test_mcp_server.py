"""Smoke tests: MCP server can be imported and tools are registered."""
import asyncio
import sys
import os
import json

import pytest

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from mcp_server import app, list_tools, call_tool


@pytest.fixture
def tools():
    return asyncio.run(list_tools())


def test_server_has_expected_tools(tools):
    tool_names = {t.name for t in tools}
    expected = {
        "get_kpis",
        "get_anomalies",
        "get_trust_score",
        "get_double_counting_audit",
        "simulate_harmonization",
        "get_neutrality_audit",
        "get_methodology_comparison",
    }
    assert expected.issubset(tool_names), f"Missing tools: {expected - tool_names}"


def test_tool_count(tools):
    assert len(tools) == 7


def test_all_tools_have_input_schema(tools):
    for tool in tools:
        assert tool.inputSchema is not None, f"Tool {tool.name} missing inputSchema"
        assert tool.inputSchema["type"] == "object"


def test_call_get_kpis():
    result = asyncio.run(call_tool("get_kpis", {"days": 14, "mode": "mock"}))
    assert len(result) == 1
    assert result[0].type == "text"
    data = json.loads(result[0].text)
    assert "spend_total_eur" in data
    assert "roas_unified" in data
    assert "breakdown_by_rmn" in data


def test_call_get_anomalies():
    result = asyncio.run(call_tool("get_anomalies", {"days": 14, "mode": "mock"}))
    data = json.loads(result[0].text)
    assert "anomalies" in data


def test_call_get_trust_score():
    result = asyncio.run(call_tool("get_trust_score", {"days": 14, "mode": "mock"}))
    data = json.loads(result[0].text)
    assert len(data) >= 1


def test_call_get_double_counting_audit():
    result = asyncio.run(
        call_tool("get_double_counting_audit", {"days": 14, "mode": "mock"})
    )
    data = json.loads(result[0].text)
    assert "total_attributed" in data
    assert "estimated_real" in data
    assert "overlap_pct" in data


def test_call_get_methodology_comparison():
    result = asyncio.run(call_tool("get_methodology_comparison", {}))
    data = json.loads(result[0].text)
    assert "amazon" in data
    assert "criteo" in data
    assert "unlimitail" in data


def test_call_simulate_harmonization():
    result = asyncio.run(
        call_tool(
            "simulate_harmonization",
            {"target_window_days": 7, "days": 14, "mode": "mock"},
        )
    )
    data = json.loads(result[0].text)
    assert "before" in data
    assert "after" in data
    assert "delta_per_network" in data


def test_call_get_neutrality_audit():
    result = asyncio.run(
        call_tool("get_neutrality_audit", {"days": 14, "mode": "mock"})
    )
    data = json.loads(result[0].text)
    assert "per_product" in data
    assert "shares_avg_pct" in data


def test_call_unknown_tool():
    result = asyncio.run(call_tool("nonexistent_tool", {}))
    data = json.loads(result[0].text)
    assert "error" in data


def test_server_name():
    assert app.name == "openrmn"
