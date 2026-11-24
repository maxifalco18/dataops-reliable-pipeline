"""Alerting: console by default, Slack-compatible webhook when configured."""

from __future__ import annotations

import json
import urllib.request
from typing import Protocol

from .logging_utils import get_logger
from .retry import retry

log = get_logger(__name__)


class Notifier(Protocol):
    def send(self, title: str, body: str, severity: str) -> None: ...


class ConsoleNotifier:
    def send(self, title: str, body: str, severity: str) -> None:
        log.warning("ALERT", extra={"ctx": {"alert": title, "severity": severity, "body": body}})


class WebhookNotifier:
    """Posts `{"text": ...}`, the payload Slack / Teams-compatible webhooks accept."""

    def __init__(self, url: str, timeout: float = 5.0):
        self.url = url
        self.timeout = timeout

    @retry(attempts=3, base_delay=1.0, exceptions=(OSError,))
    def send(self, title: str, body: str, severity: str) -> None:
        data = json.dumps({"text": f"[{severity.upper()}] {title}\n{body}"}).encode()
        req = urllib.request.Request(
            self.url, data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=self.timeout):
            pass


def build_notifier(webhook_url: str) -> Notifier:
    return WebhookNotifier(webhook_url) if webhook_url else ConsoleNotifier()
