"""Tests for config loading."""

from __future__ import annotations

import pytest

from cluster_api.config import load_config, parse_memory_bytes


class TestParseMemoryBytes:
    def test_gigabytes(self):
        assert parse_memory_bytes("8 GB") == 8 * 1024**3

    def test_megabytes(self):
        assert parse_memory_bytes("512 MB") == 512 * 1024**2

    def test_kilobytes(self):
        assert parse_memory_bytes("1024 KB") == 1024 * 1024

    def test_bytes(self):
        assert parse_memory_bytes("1024 B") == 1024

    def test_terabytes(self):
        assert parse_memory_bytes("1 TB") == 1024**4

    def test_case_insensitive(self):
        assert parse_memory_bytes("8 gb") == 8 * 1024**3

    def test_no_space(self):
        assert parse_memory_bytes("8GB") == 8 * 1024**3

    def test_fractional(self):
        assert parse_memory_bytes("1.5 GB") == int(1.5 * 1024**3)

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_memory_bytes("lots of memory")


class TestLoadConfig:
    def test_defaults(self):
        config = load_config()
        assert config.executor == "local"
        assert config.poll_interval == 10.0
        assert config.job_name_prefix is None
        assert config.zombie_timeout_minutes == 30.0
        assert config.completed_retention_minutes == 10.0
        assert config.command_timeout == 100.0
        assert config.suppress_job_email is True

    def test_from_yaml(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "executor: lsf\n"
            "queue: normal\n"
            "memory: '8 GB'\n"
            "poll_interval: 5\n"
            "job_name_prefix: myapp\n"
        )
        config = load_config(path=str(config_file))
        assert config.executor == "lsf"
        assert config.queue == "normal"
        assert config.memory == "8 GB"
        assert config.poll_interval == 5.0
        assert config.job_name_prefix == "myapp"

    def test_profile(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "executor: local\n"
            "poll_interval: 10\n"
            "profiles:\n"
            "  janelia_lsf:\n"
            "    executor: lsf\n"
            "    queue: normal\n"
            "    memory: '8 GB'\n"
        )
        config = load_config(path=str(config_file), profile="janelia_lsf")
        assert config.executor == "lsf"
        assert config.queue == "normal"
        assert config.memory == "8 GB"
        assert config.poll_interval == 10.0

    def test_overrides(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("executor: local\nqueue: short\n")
        config = load_config(
            path=str(config_file),
            overrides={"executor": "lsf", "queue": "long"},
        )
        assert config.executor == "lsf"
        assert config.queue == "long"

    def test_profile_plus_overrides(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "executor: local\n"
            "profiles:\n"
            "  prod:\n"
            "    executor: lsf\n"
            "    queue: normal\n"
        )
        config = load_config(
            path=str(config_file),
            profile="prod",
            overrides={"queue": "priority"},
        )
        assert config.executor == "lsf"
        assert config.queue == "priority"

    def test_unknown_keys_ignored(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("executor: local\nunknown_key: value\n")
        config = load_config(path=str(config_file))
        assert config.executor == "local"

    def test_zombie_and_retention_config(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "zombie_timeout_minutes: 60\n"
            "completed_retention_minutes: 30\n"
            "command_timeout: 200\n"
        )
        config = load_config(path=str(config_file))
        assert config.zombie_timeout_minutes == 60.0
        assert config.completed_retention_minutes == 30.0
        assert config.command_timeout == 200.0

    def test_script_prologue_epilogue(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "script_prologue:\n"
            "  - 'module load java/11'\n"
            "  - 'export FOO=bar'\n"
            "script_epilogue:\n"
            "  - 'echo done'\n"
        )
        config = load_config(path=str(config_file))
        assert config.script_prologue == ["module load java/11", "export FOO=bar"]
        assert config.script_epilogue == ["echo done"]

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_config(path="/nonexistent/config.yaml")

    def test_no_path_returns_defaults(self):
        config = load_config()
        assert config.executor == "local"

    def test_gpu_config(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("executor: lsf\ngpus: 2\n")
        config = load_config(path=str(config_file))
        assert config.gpus == 2

    def test_env_var_config(self, tmp_path, monkeypatch):
        config_file = tmp_path / "env_config.yaml"
        config_file.write_text("executor: lsf\nqueue: envqueue\n")
        monkeypatch.setenv("CLUSTER_API_CONFIG", str(config_file))
        config = load_config()
        assert config.executor == "lsf"
        assert config.queue == "envqueue"
