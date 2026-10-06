"""Gate regressions exercise subprocess failure and authored warning rejection."""

import json
from pathlib import Path
import tempfile
import unittest

import check
import quality


class GateTest(unittest.TestCase):
    def test_failed_command_retains_failure_and_stops(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            items = [
                check.Check("failure", ".", ("python3", "-c", "raise SystemExit(23)")),
                check.Check(
                    "must-not-run", ".", ("python3", "-c", "raise SystemExit(0)")
                ),
            ]
            self.assertFalse(check.run_checks(root, items, root))
            results = json.loads((root / "results.json").read_text())
            self.assertEqual(
                [(item["name"], item["exit_code"]) for item in results],
                [("failure", 23)],
            )
            self.assertFalse((root / "must-not-run.log").exists())

    def test_empty_suite_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "no checks selected"):
                check.run_checks(Path(folder), [], Path(folder))

    def test_missing_executable_is_retained_as_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertFalse(
                check.run_checks(
                    root, [check.Check("missing", ".", (str(root / "absent"),))], root
                )
            )
            self.assertEqual(
                json.loads((root / "results.json").read_text())[0]["exit_code"], 127
            )

    def test_sibling_pins_require_exact_complete_known_revisions(self):
        valid = "sinal=" + "a" * 40 + "\njson_blueprint=" + "b" * 40 + "\n"
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / "sibling-revisions.txt"
            path.write_text(valid)
            self.assertEqual(set(check.pins(root)), {"sinal", "json_blueprint"})
            for text in (
                "",
                "unknown=" + "a" * 40,
                valid + "sinal=" + "a" * 40,
                valid.replace("b" * 40, "bad"),
                valid.replace("sinal=", "sinal=="),
            ):
                with self.subTest(pins=text), self.assertRaises(ValueError):
                    path.write_text(text)
                    check.pins(root)

    def test_authored_erlang_warning_is_fatal_in_test_sources(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "src").mkdir()
            (root / "test").mkdir()
            (root / "src/accepted.erl").write_text(
                "-module(accepted).\n-export([value/0]).\nvalue() -> 2.\n"
            )
            self.assertEqual(quality.native(root), 0)
            (root / "test/rejected.erl").write_text(
                "-module(rejected).\n-export([value/0]).\nvalue() -> Unused = 1, 2.\n"
            )
            self.assertNotEqual(quality.native(root), 0)

    def test_added_package_requires_a_gate_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for package in (*check.PACKAGES, "examples/unowned"):
                directory = root / package
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "gleam.toml").write_text('name = "fixture"\n')
            with self.assertRaisesRegex(ValueError, "examples/unowned"):
                check.checks(root, "ci")

    def test_missing_local_dependency_fails_by_name(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for package in check.PACKAGES:
                directory = root / package
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "gleam.toml").write_text(
                    'name = "fixture"\n[dependencies]\nsinal = { path = "absent" }\n'
                )
            with self.assertRaisesRegex(ValueError, "missing sibling/package sinal"):
                check.dependencies(root)


if __name__ == "__main__":
    unittest.main()
