from pathlib import Path

import jinja2


def read_package_template(package_name: str, relative_path: str | Path) -> jinja2.Template:
    env = make_jinja_package_environment(package_name=package_name, package_path="")
    return env.get_template(str(relative_path))


def make_jinja_package_environment(package_name: str, package_path: str = "") -> jinja2.Environment:
    # package_path: Use empty string to load from the package directory itself
    # A package loader must be used for using as a library!
    return jinja2.Environment(
        loader=jinja2.PackageLoader(package_name, package_path),
        trim_blocks=True,  # better whitespace handling
        lstrip_blocks=True,
    )
