from pathlib import Path
import json
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_required_documentation_files_exist():
    required_files = [
        "README.md",
        "ARCHITECTURE.md",
        "docs/adr/ADRS.md",
        ".gitignore",
        ".env.example",
        "LICENSE",
        "dapr/components/secrets.example.json",
        "docker-compose.yml",
        "Dockerfile",
        "requirements.txt",
    ]

    for relative_path in required_files:
        assert (ROOT / relative_path).exists(), f"Missing required file: {relative_path}"


def test_real_secret_files_are_not_tracked_by_git():
    tracked_files = subprocess.check_output(
        ["git", "ls-files"],
        cwd=ROOT,
        text=True,
    ).splitlines()

    forbidden_files = {
        ".env",
        "dapr/components/secrets.json",
    }

    for forbidden_file in forbidden_files:
        assert forbidden_file not in tracked_files, f"Secret file is tracked by Git: {forbidden_file}"


def test_secret_example_does_not_contain_real_api_key():
    example_path = ROOT / "dapr/components/secrets.example.json"

    with example_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    serialized = json.dumps(data)

    assert "sk-" not in serialized
    assert "replace" in serialized.lower() or "your" in serialized.lower()