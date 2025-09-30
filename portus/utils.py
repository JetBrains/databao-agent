import datetime
from pathlib import Path

import jinja2

from portus import __file__ as PORTUS_PATH

PROJECT_ROOT = Path(PORTUS_PATH).parent.parent


def get_today_date_str() -> str:
    return datetime.datetime.now().strftime("%A, %Y-%m-%d")


def read_prompt_template(relative_path: Path) -> jinja2.Template:

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader([PROJECT_ROOT / "prompts", PROJECT_ROOT / "resources"]),
        trim_blocks=True,  # better whitespace handling
        lstrip_blocks=True,
    )
    template = env.get_template(str(relative_path))
    return template


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"