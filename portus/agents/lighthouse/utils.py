import datetime
from pathlib import Path

import jinja2

from portus._template_utils import make_jinja_package_environment

_jinja_prompts_env: jinja2.Environment | None = None


def get_today_date_str() -> str:
    return datetime.datetime.now().strftime("%A, %Y-%m-%d")


def read_prompt_template(relative_path: Path) -> jinja2.Template:
    global _jinja_prompts_env
    if _jinja_prompts_env is None:
        _jinja_prompts_env = make_jinja_package_environment(package_name="portus.agents.lighthouse")
    return _jinja_prompts_env.get_template(str(relative_path))


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"
