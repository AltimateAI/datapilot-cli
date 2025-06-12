import json
import re
from http.server import BaseHTTPRequestHandler
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request
from urllib.request import urlopen

import click


class KnowledgeBaseHandler(BaseHTTPRequestHandler):
    """HTTP request handler for serving knowledge bases and health checks."""

    token: str = ""
    instance_name: str = ""
    backend_url: str = ""

    def do_GET(self):
        """Handle GET requests."""
        path = urlparse(self.path).path

        # Match /knowledge_bases/{uuid} pattern
        match = re.match(r"^/kb/([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})$", path)

        if match:
            public_id = match.group(1)
            self.handle_knowledge_base(public_id)
        elif path == "/health":
            self.handle_health()
        else:
            self.send_error(404, "Not Found")

    def handle_knowledge_base(self, public_id):
        """Fetch and return knowledge base data."""
        url = f"{self.backend_url}/knowledge_bases/private/{public_id}"

        # Validate URL scheme for security
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ("http", "https"):
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            error_msg = json.dumps({"error": "Invalid URL scheme. Only HTTP and HTTPS are allowed."})
            self.wfile.write(error_msg.encode("utf-8"))
            return

        headers = {"Authorization": f"Bearer {self.token}", "X-Tenant": self.instance_name, "Content-Type": "application/json"}

        req = Request(url, headers=headers)  # noqa: S310

        try:
            # URL scheme validated above - only HTTP/HTTPS allowed
            with urlopen(req, timeout=30) as response:  # noqa: S310
                data = response.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)
        except HTTPError as e:
            error_body = e.read()
            error_data = error_body.decode("utf-8") if error_body else '{"error": "HTTP Error"}'
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(error_data.encode("utf-8"))
        except URLError as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            error_msg = json.dumps({"error": str(e)})
            self.wfile.write(error_msg.encode("utf-8"))

    def handle_health(self):
        """Handle health check endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))

    def log_message(self, format, *args):
        """Override to use click.echo for logging."""
        click.echo(f"{self.address_string()} - {format % args}")
