from http.server import HTTPServer

import click

from datapilot.cli.decorators import auth_options
from datapilot.clients.altimate.utils import validate_credentials

from .server import KnowledgeBaseHandler


@click.group(name="knowledge")
def cli():
    """knowledge specific commands."""


@cli.command()
@auth_options
@click.option("--port", default=4000, help="Port to run the server on")
def serve(token, instance_name, backend_url, port):
    """Serve knowledge bases via HTTP server."""
    if not token or not instance_name:
        click.echo(
            "Error: API token and instance name are required. Use --token and --instance-name options or set them in config.", err=True
        )
        raise click.Abort

    if not validate_credentials(token, backend_url, instance_name):
        click.echo("Error: Invalid credentials.", err=True)
        raise click.Abort

    # Set context data for the handler
    KnowledgeBaseHandler.token = token
    KnowledgeBaseHandler.instance_name = instance_name
    KnowledgeBaseHandler.backend_url = backend_url

    server_address = ("", port)
    httpd = HTTPServer(server_address, KnowledgeBaseHandler)

    click.echo(f"Starting knowledge base server on port {port}...")
    click.echo(f"Backend URL: {backend_url}")
    click.echo(f"Instance: {instance_name}")
    click.echo(f"Server running at http://localhost:{port}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        click.echo("\nShutting down server...")
        httpd.shutdown()
