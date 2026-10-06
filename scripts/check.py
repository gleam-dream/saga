#!/usr/bin/env python3
"""One registry for Saga's deterministic checks and retained measurements."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import tomllib

import quality

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = (".", "examples/order_consumer", "bench", "integrations/saga_postgres")


@dataclass(frozen=True)
class Check:
    name: str
    directory: str
    command: tuple[str, ...]


def pins(root: Path) -> dict[str, str]:
    values = {}
    for line in (root / "sibling-revisions.txt").read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(sinal|json_blueprint)=([a-f0-9]{40})", line)
        if not match or match[1] in values:
            raise ValueError("invalid or duplicate sibling revision entry")
        values[match[1]] = match[2]
    if set(values) != {"sinal", "json_blueprint"}:
        raise ValueError(
            "missing sibling revisions: sinal and json_blueprint are required"
        )
    return values


def checks(root: Path, profile: str) -> list[Check]:
    found = {
        str(path.parent.relative_to(root))
        for path in root.rglob("gleam.toml")
        if not {"build", "deps", "_build"}.intersection(path.relative_to(root).parts)
    }
    if found != set(PACKAGES):
        raise ValueError(
            f"package gate mismatch: added={found - set(PACKAGES)}, missing={set(PACKAGES) - found}"
        )
    static = Check(
        "formatting-and-static", ".", ("python3", "-B", "scripts/check.py", "static")
    )
    design = Check(
        "design-layers",
        ".",
        (
            "nix",
            "run",
            ".#design-gate-check",
            "--",
            "docs/design",
            ".",
            "--nested-project",
            "integrations/saga_postgres",
        ),
    )
    package_checks = {}
    for package in PACKAGES:
        name = "core" if package == "." else package.replace("/", "-")
        formats = ("src",) if package == "bench" else ("src", "test")
        items = [
            Check(f"{name}-format", package, ("gleam", "format", "--check", *formats)),
            Check(
                f"{name}-build",
                package,
                ("python3", "-B", str(root / "scripts/check.py"), "build"),
            ),
        ]
        if package != "bench":
            command = (
                ("bash", "scripts/test-postgres.sh")
                if package == "integrations/saga_postgres"
                else ("gleam", "test")
            )
            items.append(Check(f"{name}-tests", package, command))
        if package == "examples/order_consumer":
            items.append(Check("external-consumer-run", package, ("gleam", "run")))
        package_checks[package] = items
    if profile == "design":
        return [design]
    if profile == "oracle":
        return [
            static,
            *package_checks["."][:2],
            Check("reactor-oracle", ".", ("bash", "scripts/oracle.sh")),
        ]
    if profile == "benchmark":
        return [
            static,
            *package_checks["bench"],
            Check("benchmark-run", "bench", ("gleam", "run")),
        ]
    selected = [
        static,
        Check(
            "gate-tests",
            ".",
            (
                "python3",
                "-B",
                "-m",
                "unittest",
                "discover",
                "-s",
                "scripts",
                "-p",
                "test_*.py",
            ),
        ),
    ]
    for package in (".",) if profile == "fast" else PACKAGES:
        selected.extend(package_checks[package])
    if profile != "fast":
        selected.extend(
            [
                Check("compiler-contract", ".", ("bash", "scripts/check_negative.sh")),
                Check("vm-restart", ".", ("bash", "scripts/check_durable_restart.sh")),
            ]
        )
    if profile == "full":
        selected.append(design)
    return selected


def dependencies(root: Path) -> list[dict]:
    sources = {}
    pending = [root / package for package in PACKAGES]
    visited = set()
    while pending:
        directory = pending.pop().resolve()
        if directory in visited:
            continue
        visited.add(directory)
        document = tomllib.loads((directory / "gleam.toml").read_text())
        for section in ("dependencies", "dev-dependencies"):
            for name, spec in document.get(section, {}).items():
                if not isinstance(spec, dict) or "path" not in spec:
                    continue
                source = (directory / spec["path"]).resolve()
                manifest = source / "gleam.toml"
                if not manifest.is_file():
                    raise ValueError(
                        f"{directory} needs missing sibling/package {name}: {source}"
                    )
                source_manifest = tomllib.loads(manifest.read_text())
                if source_manifest["name"] != name:
                    raise ValueError(f"{name} resolves to wrong package at {source}")
                result = subprocess.run(
                    ["git", "-C", str(source), "rev-parse", "HEAD"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                sources[str(source)] = {
                    "name": name,
                    "path": str(source),
                    "revision": result.stdout.strip()
                    if result.returncode == 0
                    else "unversioned",
                }
                pending.append(source)
    return list(sources.values())


def environment(root: Path) -> dict:
    def output(command: list[str]) -> str:
        result = subprocess.run(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return result.stdout.strip()

    locks = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (
            root / "flake.lock",
            root / "manifest.toml",
            root / "bench/manifest.toml",
            root / "oracle/reactor/mix.lock",
        )
        if path.is_file()
    }
    cpu = "unknown"
    if Path("/proc/cpuinfo").is_file():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith(("model name", "Hardware", "Model")) and ":" in line:
                cpu = line.split(":", 1)[1].strip()
                break
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "revision": output(["git", "rev-parse", "HEAD"]),
        "dirty": bool(output(["git", "status", "--porcelain"])),
        "system": platform.platform(),
        "cpu": cpu,
        "load": os.getloadavg(),
        "gleam": output(["gleam", "--version"]),
        "erlang": output(
            [
                "erl",
                "-noshell",
                "-eval",
                'io:format("OTP ~s; ERTS ~s; schedulers ~p~n", [erlang:system_info(otp_release), erlang:system_info(version), erlang:system_info(schedulers_online)]), halt().',
            ]
        ),
        "lock_sha256": locks,
    }


def run_checks(root: Path, selected: list[Check], logs: Path) -> bool:
    if not selected:
        raise ValueError("no checks selected")
    logs.mkdir(parents=True, exist_ok=True)
    results = []
    (logs / "results.json").write_text("[]\n")
    for item in selected:
        print(f"Checking {item.name}", flush=True)
        started = time.monotonic()
        log = logs / f"{item.name}.log"
        with log.open("w") as output:
            try:
                environment = dict(os.environ)
                if item.name == "reactor-oracle":
                    environment["SAGA_ORACLE_EVIDENCE_DIR"] = str(
                        logs / "oracle-evidence"
                    )
                status = subprocess.run(
                    item.command,
                    cwd=root / item.directory,
                    env=environment,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                ).returncode
            except OSError as error:
                output.write(str(error) + "\n")
                status = 127
        results.append(
            {
                "name": item.name,
                "command": list(item.command),
                "directory": item.directory,
                "exit_code": status,
                "seconds": round(time.monotonic() - started, 3),
            }
        )
        (logs / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        if status:
            print(f"FAILED {item.name}; see {log}\n{log.read_text()}", flush=True)
            return False
    print(f"Passed {len(results)} checks. Evidence: {logs}", flush=True)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile",
        choices=(
            "fast",
            "full",
            "ci",
            "design",
            "oracle",
            "benchmark",
            "static",
            "build",
            "pins",
        ),
    )
    parser.add_argument("--logs", type=Path)
    arguments = parser.parse_args()
    if arguments.profile == "pins":
        try:
            for name, revision in pins(ROOT).items():
                print(f"{name}={revision}")
        except (ValueError, OSError) as error:
            raise SystemExit(str(error)) from error
        return
    if arguments.profile == "static":
        raise SystemExit(quality.static(ROOT))
    if arguments.profile == "build":
        raise SystemExit(quality.build(Path.cwd()))
    logs = (arguments.logs or ROOT / ".artifacts" / arguments.profile).resolve()
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "results.json").write_text("[]\n")
    try:
        selected = checks(ROOT, arguments.profile)
        pins(ROOT)
        sources = [] if arguments.profile == "design" else dependencies(ROOT)
    except (ValueError, OSError, KeyError) as error:
        (logs / "preflight.log").write_text(str(error) + "\n")
        raise SystemExit(str(error)) from error
    (logs / "dependencies.json").write_text(json.dumps(sources, indent=2) + "\n")
    (logs / "environment.json").write_text(
        json.dumps(environment(ROOT), indent=2) + "\n"
    )
    raise SystemExit(0 if run_checks(ROOT, selected, logs) else 1)


if __name__ == "__main__":
    main()
