#!/usr/bin/env python3
"""
Utility script to synchronize package __version__ and __app_name__ in __init__.py
with the versions specified in pyproject.toml for each benchmark project.
"""

import re
from pathlib import Path

import tomllib

PROJECTS = [
    {
        "name": "engine-bench",
        "pkg": "engine_bench",
        "path": Path("engine-bench"),
    },
    {
        "name": "hive-bench",
        "pkg": "hive_bench",
        "path": Path("hive-bench"),
    },
]


def update_project(proj):
    pyproject_file = proj["path"] / "pyproject.toml"
    init_file = proj["path"] / "src" / proj["pkg"] / "__init__.py"
    if not pyproject_file.exists() or not init_file.exists():
        print(f"Skipping {proj['name']}: missing files")
        return

    # Parse version and name from pyproject.toml
    with open(pyproject_file, "rb") as f:
        data = tomllib.load(f)
    version = data.get("project", {}).get("version")
    name = data.get("project", {}).get("name")
    if not version or not name:
        print(f"Skipping {proj['name']}: missing name/version in pyproject.toml")
        return

    content = init_file.read_text(encoding="utf-8")
    lines = content.splitlines(keepends=True)

    # Patterns
    version_re = re.compile(r"^__version__\s*=")
    app_re = re.compile(r"^__app_name__\s*=")

    # Flags
    saw_app = False
    new_lines = []
    for line in lines:
        if app_re.match(line):
            new_lines.append(f'__app_name__ = "{name}"\n')
            saw_app = True
        elif version_re.match(line):
            new_lines.append(f'__version__ = "{version}"\n')
        else:
            new_lines.append(line)

    # Insert __app_name__ if not seen (before version)
    if not saw_app:
        out_lines = []
        for line in new_lines:
            if version_re.match(line):
                out_lines.append(f'__app_name__ = "{name}"\n')
                saw_app = True
            out_lines.append(line)
        new_lines = out_lines

    # Write back
    init_file.write_text("".join(new_lines), encoding="utf-8")
    print(f"Updated {proj['pkg']}/__init__.py: name={name}, version={version}")


def main():
    for proj in PROJECTS:
        update_project(proj)


if __name__ == "__main__":
    main()
