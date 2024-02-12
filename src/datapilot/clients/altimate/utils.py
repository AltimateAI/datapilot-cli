import os
from typing import Optional

import click


def check_token_and_instance(
    token: Optional[str],
    instance_name: Optional[str],
):
    if not token:
        token = os.environ.get("ALTIMATE_API_KEY")

    if not instance_name:
        instance_name = os.environ.get("ALTIMATE_INSTANCE_NAME")

    if not token or not instance_name:
        click.echo(
            "Error: API TOKEN and instance name is required. Please provide a valid API token."
            " You can pass it as command line arguments or set it using environment variables like "
            "ALTIMATE_API_KEY and ALTIMATE_INSTANCE_NAME."
        )
        return
