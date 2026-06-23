"""Producer: read the live Wikimedia recent-changes SSE firehose -> Kinesis.

Wikimedia EventStreams is an open, no-auth Server-Sent-Events endpoint that
emits a constant stream of every edit across all wikis — a perfect free,
high-throughput source. This runs as a small always-on task (locally, or on
Fargate/EC2 in AWS); it is NOT a Lambda, because Lambda can't hold a long-lived
streaming HTTP connection.

    STREAM_NAME=my-stream python src/producer.py        # -> real Kinesis
    (or use scripts/local_run.py for the no-AWS path)
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any, Iterator

import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HTTP_TIMEOUT_S = 30


def stream_events(
    url: str | None = None,
    max_events: int | None = None,
    opener=urllib.request.urlopen,
) -> Iterator[dict[str, Any]]:
    """Yield raw event dicts parsed from the SSE stream.

    SSE framing: lines beginning ``data:`` carry the payload; a blank line ends
    an event; lines beginning ``:`` are comments/keep-alives. We accumulate the
    data lines of each event and JSON-parse them.
    """
    url = url or common.SSE_URL
    req = urllib.request.Request(url, headers={"User-Agent": "streaming-pipeline/1.0"})
    count = 0
    data_buf: list[str] = []
    with opener(req, timeout=HTTP_TIMEOUT_S) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            if line == "":  # event boundary
                if data_buf:
                    payload = "\n".join(data_buf)
                    data_buf = []
                    try:
                        yield json.loads(payload)
                        count += 1
                    except json.JSONDecodeError:
                        logger.debug("skipping unparseable SSE payload")
                    if max_events is not None and count >= max_events:
                        return
                continue
            if line.startswith(":"):  # comment / keep-alive
                continue
            if line.startswith("data:"):
                data_buf.append(line[len("data:"):].lstrip())


def run(
    stream: common.StreamBackend,
    max_events: int | None = None,
    events: Iterator[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Pump events from the SSE source into the stream backend."""
    source = events if events is not None else stream_events(max_events=max_events)
    sent = 0
    for ev in source:
        # Partition by wiki so a single wiki's events keep ordering on one shard.
        stream.put(ev, partition_key=str(ev.get("wiki") or ev.get("server_name") or "unknown"))
        sent += 1
        if sent % 500 == 0:
            logger.info("produced %d events", sent)
    stream.flush()
    logger.info("producer finished: %d events", sent)
    return {"produced": sent}


def main() -> int:
    stream_name = os.environ.get("STREAM_NAME")
    if not stream_name:
        raise SystemExit("set STREAM_NAME to the Kinesis data stream name")
    max_events = int(os.environ["MAX_EVENTS"]) if os.environ.get("MAX_EVENTS") else None
    stream = common.KinesisStream(stream_name)
    run(stream, max_events=max_events)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
