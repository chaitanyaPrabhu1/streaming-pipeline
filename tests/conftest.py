import base64
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import common  # noqa: E402

# A fixed "now" so partition keys / batch ids are deterministic in tests.
NOW = dt.datetime(2024, 5, 1, 12, 0, 0)


def fake_event(
    event_id="11111111-1111-1111-1111-111111111111",
    type="edit",
    wiki="enwiki",
    domain="en.wikipedia.org",
    timestamp=1714564800,  # 2024-05-01T12:00:00Z
    user="Alice",
    bot=False,
    minor=False,
    namespace=0,
    old=100,
    new=150,
    title="Example",
):
    """A Wikimedia recent-change event, shaped like the real SSE payload."""
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": event_id, "domain": domain, "stream": "mediawiki.recentchange"},
        "id": 999,
        "type": type,
        "namespace": namespace,
        "title": title,
        "timestamp": timestamp,
        "user": user,
        "bot": bot,
        "minor": minor,
        "length": {"old": old, "new": new},
        "server_name": domain,
        "wiki": wiki,
    }


def as_kinesis_event(raw_events):
    """Wrap raw event dicts in a Lambda Kinesis event envelope (base64 data)."""
    return {
        "Records": [
            {"kinesis": {"data": base64.b64encode(json.dumps(e).encode()).decode()}}
            for e in raw_events
        ]
    }


@pytest.fixture
def local_backend(tmp_path):
    return common.LocalBackend(str(tmp_path / "lake"))
