from typing import TYPE_CHECKING, Any

from parsl.executors.base import ParslExecutor
from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

try:
    from parsl.providers.base import ExecutionProvider
except ImportError:
    from parsl.providers.provider_base import ExecutionProvider  # type: ignore

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value, \
    get_workflow_name
from lsst.ctrl.bps.parsl.site import SiteConfig

if TYPE_CHECKING:
    from lsst.ctrl.bps.parsl.job import ParslJob


__all__ = ["TaskVine"]


class TaskVine(SiteConfig):
    def __init__(self, *args, **kwargs):
        # Have BPS-defined resource requests for each job passed to task_vine.
        kwargs["resource_list"] = ["memory", "cores", "disk",
                                   "running_time_min", "priority"]
        super().__init__(*args, **kwargs)

    def make_executor(
            self,
            label: str,
            provider: ExecutionProvider,
    ) -> ParslExecutor:
        """Return a `TaskVineExecutor`.

        Parameters
        ----------
        label : `str`
            Label for executor.
        provider : `ExecutionProvider`
            Parsl execution provider, e.g., `SlurmProvider`.
        """
        manager_config = TaskVineManagerConfig(address=self.get_address())
        return TaskVineExecutor(
            label=label,
            worker_launch_method="provider",
            manager_config=manager_config,
            provider=provider,
        )

    def get_executors(self) -> list[ParslExecutor]:
        return [self.make_executor("task_vine", self.get_provider())]

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        return "task_vine"

    def get_provider(
            self,
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
        nodes = get_bps_config_value(self.site, "nodes_per_block", int, 1)
        cores_per_node = get_bps_config_value(self.site, "cores_per_node",
                                              int, cores_per_node)
        walltime = get_bps_config_value(self.site, "walltime", str, walltime,
                                        required=True)
        mem_per_node = get_bps_config_value(self.site, "mem_per_node", int,
                                            mem_per_node)
        qos = get_bps_config_value(self.site, "qos", str, qos)
        constraint = get_bps_config_value(self.site, "constraint", str,
                                          constraint)
        singleton = get_bps_config_value(self.site, "singleton", bool,
                                         singleton)
        exclusive = get_bps_config_value(self.site, "exclusive", bool,
                                         exclusive)
        scheduler_options = get_bps_config_value(
            self.site, "scheduler_options", str, scheduler_options)

        # Replace any filepath separators with underscores since Parsl
        # creates a shell script named f"cmd_{job_name}.sh" using the
        # --job-name value in the sbatch script.
        job_name = get_workflow_name(self.config).replace("/", "_")
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
