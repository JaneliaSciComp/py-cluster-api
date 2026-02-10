"""Utility functions for rendering and writing job scripts."""

from __future__ import annotations

import re
from pathlib import Path

from .config import ClusterConfig

_SCRIPT_TEMPLATE = """\
%(shebang)s
%(job_header)s
%(prologue)s
%(command)s
%(epilogue)s
"""


def render_script(
    config: ClusterConfig,
    command: str,
    header_lines: list[str],
    prologue: list[str] | None = None,
    epilogue: list[str] | None = None,
) -> str:
    """Render a job script from the template.

    Args:
        config: Cluster configuration (provides shebang, directives_skip,
            extra_directives, script_prologue, script_epilogue).
        command: The shell command to run.
        header_lines: Scheduler directive lines (e.g. from build_header).
        prologue: Per-job prologue lines appended after config prologue.
        epilogue: Per-job epilogue lines appended after config epilogue.
    """
    # Filter via directives_skip
    skip = set(config.directives_skip)
    if skip:
        header_lines = [
            line
            for line in header_lines
            if not any(s in line for s in skip)
        ]
    # Extend with extra_directives
    header_lines = [*header_lines, *config.extra_directives]

    all_prologue = list(config.script_prologue)
    if prologue:
        all_prologue.extend(prologue)

    all_epilogue = list(config.script_epilogue)
    if epilogue:
        all_epilogue.extend(epilogue)

    return _SCRIPT_TEMPLATE % {
        "shebang": config.shebang,
        "job_header": "\n".join(header_lines),
        "prologue": "\n".join(all_prologue),
        "command": command,
        "epilogue": "\n".join(all_epilogue),
    }


def write_script(directory: str, content: str, name: str, counter: int) -> str:
    """Write a job script to *directory* and return the path as a string.

    The file is named ``{safe_name}.{counter}.sh`` and made executable.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w\-.]", "_", name)
    script_path = directory / f"{safe_name}.{counter}.sh"
    script_path.write_text(content)
    script_path.chmod(0o755)
    return str(script_path)
