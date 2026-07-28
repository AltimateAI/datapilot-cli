"""Logging configuration for the datapilot CLI.

Verbose output is opt-in and can be turned on two ways:

- the ``DATAPILOT_DEBUG`` environment variable, which is the option to reach for
  in CI/CD pipelines where the command line is generated and hard to edit
- the ``--debug`` flag on individual commands

Both raise the root logger to ``DEBUG``, which surfaces the HTTP status codes and
API error bodies that ``APIClient`` already records but normally discards.
"""

import logging
import os
from typing import Optional
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

DEBUG_ENV_VAR = "DATAPILOT_DEBUG"

# Values that count as "on" for DATAPILOT_DEBUG. Anything else (including "0",
# "false" and the empty string) leaves debug logging off.
_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})

# Debug mode is sticky: a command-level `--debug` must not be undone by a later
# call that happens to default to False.
_debug_enabled = False


def debug_enabled_via_env() -> bool:
    """Return True when DATAPILOT_DEBUG is set to a truthy value."""
    return os.environ.get(DEBUG_ENV_VAR, "").strip().lower() in _TRUTHY_VALUES


def configure_logging(debug: bool = False) -> bool:
    """Configure root logging for the CLI and return whether debug mode is on.

    Safe to call more than once; the group callback and the command callback both
    call it, and the more verbose of the two wins.
    """
    global _debug_enabled
    _debug_enabled = _debug_enabled or debug or debug_enabled_via_env()
    level = logging.DEBUG if _debug_enabled else logging.INFO

    root = logging.getLogger()
    # Only install a handler when nothing else has, so embedding applications
    # (and pytest's caplog) keep theirs. Setting the level is what actually
    # decides whether the DEBUG records get through.
    if not root.handlers:
        logging.basicConfig(level=level)
    root.setLevel(level)

    # urllib3 logs each request line in full, which for a presigned S3 upload means
    # the AWS key and signature. Our own client logs the status codes and request
    # params, so nothing diagnostic is lost by holding urllib3 at INFO.
    logging.getLogger("urllib3").setLevel(logging.INFO)

    return _debug_enabled


def is_debug_enabled() -> bool:
    """Return whether debug logging is currently enabled."""
    return _debug_enabled


def redact_url(url: Optional[str]) -> str:
    """Strip a URL's query string so it is safe to log.

    Presigned upload URLs carry AWS credentials and a signature in the query
    string, and debug output routinely gets pasted into support tickets.
    """
    if not url:
        return ""

    parts = urlsplit(url)
    if not parts.query:
        return url

    return urlunsplit((parts.scheme, parts.netloc, parts.path, "<redacted>", ""))
