import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKS = (
    ("Ruff lint", ("ruff", "check", ".")),
    ("Ruff format", ("ruff", "format", "--check", "src", "migrations", "tests", "scripts")),
    ("Mypy", ("mypy", "--explicit-package-bases", "src")),
    ("Bandit", ("bandit", "-q", "-r", "src")),
    ("Pytest", ("pytest",)),
)


def audit_dependencies() -> int:
    with tempfile.TemporaryDirectory() as temporary_directory:
        requirements_path = Path(temporary_directory) / "requirements.txt"
        export_result = subprocess.run(
            [
                "uv",
                "export",
                "--locked",
                "--all-groups",
                "--no-emit-project",
                "--no-header",
                "--no-annotate",
                "--no-hashes",
                "--format",
                "requirements.txt",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if export_result.returncode:
            sys.stderr.write(export_result.stderr)
            return export_result.returncode
        requirements_path.write_text(export_result.stdout, encoding="utf-8")

        audit_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--strict",
                "--requirement",
                str(requirements_path),
            ],
            cwd=ROOT,
            check=False,
        )
        return audit_result.returncode


def main() -> int:
    failed_checks = []
    for name, arguments in CHECKS:
        print(f"\n==> {name}", flush=True)
        result = subprocess.run(
            [sys.executable, "-m", *arguments],
            cwd=ROOT,
            check=False,
        )
        if result.returncode:
            failed_checks.append(name)

    print("\n==> Dependency audit", flush=True)
    if audit_dependencies():
        failed_checks.append("Dependency audit")

    if failed_checks:
        print(f"\nQuality gate failed: {', '.join(failed_checks)}", file=sys.stderr)
        return 1

    print("\nAll quality checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
