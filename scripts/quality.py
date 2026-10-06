"""Static tooling and authored Erlang validation for the package gate."""

import os
from pathlib import Path
import subprocess
import tempfile


def run(command: list[str], directory: Path, **arguments: object) -> int:
    print("Running " + " ".join(command), flush=True)
    return subprocess.run(command, cwd=directory, **arguments).returncode


def static(root: Path) -> int:
    workflows = sorted(
        str(path.relative_to(root))
        for path in (root / ".github/workflows").iterdir()
        if path.suffix in (".yml", ".yaml")
    )
    shells = sorted(
        str(path.relative_to(root))
        for directory in ("scripts", "integrations")
        for path in (root / directory).rglob("*.sh")
        if "build" not in path.parts
    )
    commands = [
        ["nix", "flake", "check"],
        ["ruff", "check", "scripts"],
        ["actionlint", *workflows],
        ["shellcheck", *shells],
    ]
    for command in commands:
        status = run(command, root)
        if status:
            return status
    return 0


def native_sources(package: Path) -> list[Path]:
    return sorted(
        path
        for directory in ("src", "test")
        for path in (package / directory).rglob("*.erl")
    )


def native(package: Path) -> int:
    sources = native_sources(package)
    if not sources:
        print(f"No authored Erlang in {package}", flush=True)
        return 0
    libraries = package / "build/dev/erlang"
    include_arguments = [
        argument
        for include in sorted(libraries.glob("*/include"))
        for argument in ("-I", str(include))
    ]
    environment = dict(os.environ, ERL_LIBS=str(libraries))
    with tempfile.TemporaryDirectory(prefix="saga-native-check-") as output:
        return run(
            ["erlc", "-Werror", *include_arguments, "-o", output, *map(str, sources)],
            package,
            env=environment,
        )


def build(package: Path) -> int:
    status = run(["gleam", "build", "--warnings-as-errors"], package)
    return status if status else native(package)
