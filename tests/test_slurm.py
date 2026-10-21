"""Tests for the SLURM executor."""

from __future__ import annotations

from cluster_api._types import ResourceSpec
from cluster_api.config import ClusterConfig
from cluster_api.executors import get_executor_class
from cluster_api.executors.slurm import SlurmExecutor, slurm_format_mem


class TestRegistry:
    def test_slurm_registered(self):
        assert get_executor_class("slurm") is SlurmExecutor

    def test_top_level_export(self):
        import cluster_api
        assert cluster_api.SlurmExecutor is SlurmExecutor


class TestSlurmFormatMem:
    def test_gigabytes(self):
        assert slurm_format_mem(8 * 1024**3) == "8192M"

    def test_rounds_up(self):
        assert slurm_format_mem(512 * 1024**2 + 1) == "513M"

    def test_sub_mebibyte_rounds_to_one(self):
        assert slurm_format_mem(10) == "1M"


class TestBuildHeader:
    def test_basic_header(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        lines = executor.build_header("test-job", ResourceSpec(work_dir="/w"))
        assert "#SBATCH --job-name=test-job" in lines
        assert '#SBATCH --output="/w/stdout.%j.log"' in lines
        assert '#SBATCH --error="/w/stderr.%j.log"' in lines
        assert "#SBATCH --nodes=1" in lines
        assert "#SBATCH --ntasks=1" in lines
        assert "#SBATCH --no-requeue" in lines
        assert "#SBATCH --partition=normal" in lines
        assert "#SBATCH --mem=8192M" in lines
        assert "#SBATCH --time=04:00:00" in lines
        assert '#SBATCH --chdir="/w"' in lines
        assert not any("--array" in line for line in lines)
        assert not any("%J" in line for line in lines)

    def test_with_resources(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        res = ResourceSpec(
            cpus=4, gpus=2, memory="16 GB", walltime="08:00:00",
            queue="long", work_dir="/scratch",
            extra_directives=["--account=proj1"],
        )
        lines = executor.build_header("test-job", res)
        assert "#SBATCH --partition=long" in lines
        assert "#SBATCH --cpus-per-task=4" in lines
        assert "#SBATCH --gres=gpu:2" in lines
        assert "#SBATCH --mem=16384M" in lines
        assert "#SBATCH --time=08:00:00" in lines
        assert '#SBATCH --chdir="/scratch"' in lines
        assert "#SBATCH --account=proj1" in lines

    def test_no_optional_resources(self):
        config = ClusterConfig(executor="slurm")
        executor = SlurmExecutor(config)
        lines = executor.build_header("j", ResourceSpec(work_dir="/w"))
        joined = "\n".join(lines)
        for flag in ("--partition", "--cpus-per-task", "--gres", "--mem", "--time"):
            assert flag not in joined

    def test_config_fallbacks(self):
        config = ClusterConfig(executor="slurm", cpus=2, gpus=1, memory="1 GB",
                               walltime="00:30:00", queue="gpu")
        executor = SlurmExecutor(config)
        lines = executor.build_header("j", ResourceSpec(work_dir="/w"))
        assert "#SBATCH --cpus-per-task=2" in lines
        assert "#SBATCH --gres=gpu:1" in lines
        assert "#SBATCH --mem=1024M" in lines
        assert "#SBATCH --time=00:30:00" in lines
        assert "#SBATCH --partition=gpu" in lines

    def test_explicit_log_paths(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        res = ResourceSpec(work_dir="/w", stdout_path="/logs/o.txt", stderr_path="/logs/e.txt")
        lines = executor.build_header("j", res)
        assert '#SBATCH --output="/logs/o.txt"' in lines
        assert '#SBATCH --error="/logs/e.txt"' in lines

    def test_array_header(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        lines = executor.build_header("arr", ResourceSpec(work_dir="/w"), array_spec="1-10%3")
        assert "#SBATCH --array=1-10%3" in lines
        assert '#SBATCH --output="/w/stdout.%A_%a.log"' in lines
        assert '#SBATCH --error="/w/stderr.%A_%a.log"' in lines
