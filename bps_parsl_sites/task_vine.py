from parsl.executors.base import ParslExecutor
from parsl.executors.taskvine import (
    TaskVineExecutor,
    TaskVineManagerConfig,
    TaskVineFactoryConfig,
)
from parsl.providers.base import ExecutionProvider

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value
from lsst.ctrl.bps.parsl.job import ParslJob
from lsst.ctrl.bps.parsl.site import SiteConfig

from .utils import get_slurm_provider, get_local_provider


__all__ = ["SlurmTaskVine", "LocalTaskVine"]


class TaskVine(SiteConfig):
    """Base class configuration for `TaskVineExecutor`.

    Subclasses must provide implementations for ``.get_executors``
    and ``.select_executor``.  In ``.get_executors``, the site-specific
    `ExecutionProvider` must be defined.

    Parameters
    ----------
    *args : `~typing.Any`
        Parameters forwarded to base class constructor.
    **kwargs : `~typing.Any`
        Keyword arguments passed to base class constructor, augmented by
        the ``resource_list`` argument.
    """

    def __init__(self, *args, **kwargs):
        # Have BPS-defined resource requests for each job passed to task_vine.
        kwargs["resource_list"] = [
            "memory",
            "cores",
            "disk",
            "running_time_min",
            "priority",
        ]
        super().__init__(*args, **kwargs)

    def make_executor(
        self,
        label: str,
        provider: ExecutionProvider,
        worker_options: str | None = None,
        tv_max_retries: int = 1,
    ) -> ParslExecutor:
        """Return a `TaskVineExecutor`.

        Parameters
        ----------
        label : `str`
            Label for executor.
        provider : `ExecutionProvider`
            Parsl execution provider, e.g., `SlurmProvider`.
        worker_options : `str`, optional
            Extra options to pass to work_queue workers, e.g.,
            ``"--memory=90000"``. Default: `None`.
        tv_max_retries : `int`, optional
            Number of retries for task_vine to attempt per job.  Set to
            ``None`` to have it try indefinitely; set to ``1`` to have Parsl
            control the number of retries.  Default: ``1``.
        """
        worker_options = get_bps_config_value(
            self.site, "worker_options", str, worker_options
        )
        max_retries = get_bps_config_value(
            self.site, "tv_max_retries", int, tv_max_retries
        )
        manager_config = TaskVineManagerConfig(
            address=self.get_address(), max_retries=max_retries
        )
        factory_config = TaskVineFactoryConfig(worker_options=worker_options)
        return TaskVineExecutor(
            label=label,
            worker_launch_method="provider",
            manager_config=manager_config,
            factory_config=factory_config,
            provider=provider,
        )


class SlurmTaskVine(TaskVine):

    def get_executors(self) -> list[ParslExecutor]:
        return [self.make_executor("slurm_task_vine", get_slurm_provider(self))]

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
        return "slurm_task_vine"


class LocalTaskVine(TaskVine):

    def get_executors(self) -> list[ParslExecutor]:
        return [self.make_executor("local_task_vine", get_local_provider(self))]

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
        return "local_task_vine"
