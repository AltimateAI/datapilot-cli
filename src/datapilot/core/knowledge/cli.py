from http.server import HTTPServer

import click

from .server import KnowledgeBaseHandler


@click.group(name="knowledge")
def cli():
    """knowledge specific commands."""


@cli.command()
@click.option("--port", default=4000, help="Port to run the server on")
@click.pass_context
def serve(ctx, port):
    """Serve knowledge bases via HTTP server."""
    # Get configuration from parent context
    token = ctx.parent.obj.get("token")
    instance_name = ctx.parent.obj.get("instance_name")
    backend_url = ctx.parent.obj.get("backend_url")

    if not token or not instance_name:
        click.echo(
            "Error: API token and instance name are required. Use --token and --instance-name options or set them in config.", err=True
        )
        ctx.exit(1)

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
