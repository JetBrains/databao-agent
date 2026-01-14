"""Databao CLI - temporary CLI that will be replaced by central databao CLI later."""

import subprocess
import sys
from pathlib import Path

import click


@click.group()
def databao() -> None:
    """Databao - Natural language queries for your data."""
    pass


@databao.group()
def run() -> None:
    """Run Databao services."""
    pass


@run.command(name="app")
def run_app() -> None:
    """Launch the Databao Streamlit web interface.

    The app will detect DCE projects automatically and allow you to
    select one from the UI if multiple candidates are found.
    """
    # Get the path to the streamlit app
    streamlit_app_path = Path(__file__).parent.parent / "streamlit_app" / "app.py"

    if not streamlit_app_path.exists():
        click.echo(f"Error: Streamlit app not found at {streamlit_app_path}", err=True)
        sys.exit(1)

    click.echo("Starting Databao...")

    # Launch streamlit
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(streamlit_app_path), "--server.headless", "true"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        click.echo(f"Error running Streamlit: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nShutting down Databao...")


def main() -> None:
    """Entry point for the CLI."""
    databao()


if __name__ == "__main__":
    main()
