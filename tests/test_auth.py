"""Tests for aichat_auth token parsing and lookup behavior."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

import aichat_auth


def _make_compound_token(*, key: str = "test-key", channel: str = "ch-1", signature: str = "sig-1") -> str:
    payload = {"k": key, "c": channel, "s": signature}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


def test_parse_compound_token_accepts_valid_token():
    token = _make_compound_token(key="k1", channel="ch-abc", signature="sig")
    parsed = aichat_auth._parse_compound_token(token)
    assert parsed == ("k1", "ch-abc")


def test_parse_compound_token_rejects_trailing_junk():
    token = _make_compound_token() + "***"
    assert aichat_auth._parse_compound_token(token) is None


def test_parse_compound_token_rejects_wrong_field_types():
    payload = {"k": 123, "c": ["ch-1"], "s": {"sig": "x"}}
    token = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    assert aichat_auth._parse_compound_token(token) is None


def test_lookup_token_for_cwd_checks_root_mapping(monkeypatch, tmp_path: Path):
    tokens_path = tmp_path / "tokens.json"
    tokens_path.write_text(json.dumps({"/": "root-token"}))
    monkeypatch.setattr(aichat_auth, "TOKENS_PATH", tokens_path)
    monkeypatch.chdir(tmp_path)
    assert aichat_auth._lookup_token_for_cwd() == "root-token"


def test_get_private_key_rejects_junk_suffixed_token(compound_token):
    with pytest.raises(SystemExit):
        aichat_auth.get_private_key(token=compound_token + "***")
