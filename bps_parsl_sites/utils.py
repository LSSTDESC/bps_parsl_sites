from typing import Any

from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.providers.base import ExecutionProvider
from parsl.providers import LocalProvider

from lsst.ctrl.bps import BpsConfig

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value, get_workflow_name
from lsst.ctrl.bps.parsl.site import SiteConfig

__all__ = ["get_slurm_provider", "get_local_provider"]


def get_slurm_provider(
    site_config: SiteConfig,
    nodes: int | None = 1,
    cores_per_node: int | None = None,
    walltime: str | None = None,
    mem_per_node: int | None = None,
    qos: str | None = None,
    constraint: str | None = None,
    singleton: bool = False,
    exclusive: bool = False,
    scheduler_options: str | None = None,
    provider_options: dict[str, Any] | None = None,
) -> ExecutionProvider:
    """Return a SlurmProvider."""
    nodes = get_bps_config_value(site_config.site, "nodes_per_block", int, 1)
    cores_per_node = get_bps_config_value(
        site_config.site, "cores_per_node", int, cores_per_node
    )
    walltime = get_bps_config_value(
        site_config.site, "walltime", str, walltime, required=True
    )
    mem_per_node = get_bps_config_value(
        site_config.site, "mem_per_node", int, mem_per_node
    )
    qos = get_bps_config_value(site_config.site, "qos", str, qos)
    constraint = get_bps_config_value(site_config.site, "constraint", str, constraint)
    singleton = get_bps_config_value(site_config.site, "singleton", bool, singleton)
    exclusive = get_bps_config_value(site_config.site, "exclusive", bool, exclusive)
    scheduler_options = get_bps_config_value(
        site_config.site, "scheduler_options", str, scheduler_options
    )
    provider_options = get_bps_config_value(
        site_config.site, "provider_options", BpsConfig, provider_options
    )
    provider_options = {} if provider_options is None else dict(provider_options)
    # Replace any filepath separators with underscores since Parsl
    # creates a shell script named f"cmd_{job_name}.sh" using the
    # --job-name value in the sbatch script.
    job_name = get_workflow_name(site_config.config).replace("/", "_")
    if scheduler_options is None:
        scheduler_options = ""
    scheduler_options += "\n"
    scheduler_options += f"#SBATCH --job-name={job_name}\n"
    if qos:
        scheduler_options += f"#SBATCH --qos={qos}\n"
    if constraint:
        scheduler_options += f"#SBATCH --constraint={constraint}\n"
    if singleton:
        # The following SBATCH directives allow only a single
        # slurm job (parsl block) with our job_name to run at
        # once. This means we can have one job running, and one
        # already in the queue when the first exceeds the walltime
        # limit. More backups could be achieved with a larger
        # value of max_blocks.  This only allows one job to be
        # actively running at once, so that needs to be sized
        # appropriately by the user.
        scheduler_options += "#SBATCH --dependency=singleton\n"
    provider = SlurmProvider(
        nodes_per_block=nodes,
        cores_per_node=cores_per_node,
        mem_per_node=mem_per_node,
        walltime=walltime,
        exclusive=exclusive,
        scheduler_options=scheduler_options,
        launcher=SrunLauncher(overrides="-K0 -k --cpu-bind=none"),
        **(provider_options or {}),
    )
    return provider


def get_local_provider(site_config: SiteConfig) -> ExecutionProvider:
    """Return a LocalProvider"""
    nodes_per_block = get_bps_config_value(site_config.site, "nodes_per_block", int, 1)
    provider_options = {
        "nodes_per_block": nodes_per_block,
        "init_blocks": 1,
        "min_blocks": 0,
        "max_blocks": 1,
        "parallelism": 0,
        "cmd_timeout": 300,
    }
    if nodes_per_block > 1:
        provider_options["launcher"] = SrunLauncher(overrides="-K0 -k --cpu-bind=none")
    provider = LocalProvider(**provider_options)
    return provider
