from parsl.executors.base import ParslExecutor
from parsl.providers.base import ExecutionProvider

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value
from lsst.ctrl.bps.parsl.job import ParslJob
from lsst.ctrl.bps.parsl.sites import WorkQueue

from .utils import get_slurm_provider


__all__ = ["SlurmWorkQueue"]


class SlurmWorkQueue(WorkQueue):
    """Configuration for a `WorkQueueExecutor` that uses a `SlurmProvider`
    to manage resources.

    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``None``.
      If ``None``, then find a free port.
    - ``worker_options (`str`): Extra options to pass to work_queue workers.
      A typical option specifies the memory available per worker, e.g.,
      ``"--memory=90000"``, which sets the available memory to 90 GB.
      Default: ``""``
    - ``wq_max_retries`` (`int`): The number of retries that work_queue
      will make in case of task failures.  Set to ``None`` to have work_queue
      retry forever; set to ``1`` to have retries managed by Parsl.
      Default: ``1``
    - ``nodes_per_block`` (`int`): The number of allocated nodes.
      Default: ``1``
    """

    def make_executor(
        self,
        label: str,
        provider: ExecutionProvider,
        *,
        port: int = 0,
        worker_options: str = "",
        wq_max_retries: int = 1,
    ) -> ParslExecutor:
        """Return a `WorkQueueExecutor`.  The ``provider`` contains the
        site-specific configuration.

        Parameters
        ----------
        label : `str`
            Label for executor.
        provider : `ExecutionProvider`
            Parsl execution provider, e.g., `SlurmProvider`.
        port : `int`, optional
            Port used by work_queue.  Default: `0`
        worker_options : `str`, optional
            Extra options to pass to work_queue workers, e.g.,
            ``"--memory=90000"``. Default: `""`.
        wq_max_retries : `int`, optional
            Number of retries for work_queue to attempt per job.  Set to
            ``None`` to have it try indefinitely; set to ``1`` to have Parsl
            control the number of retries.  Default: ``1``.
        """
        port = get_bps_config_value(self.site, "port", int, port)
        return super().make_executor(
            label,
            provider,
            port=port,
            worker_options=worker_options,
            wq_max_retries=wq_max_retries,
        )

    def get_executors(self) -> list[ParslExecutor]:
        return [self.make_executor("slurm_work_queue", get_slurm_provider(self))]

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
        return "slurm_work_queue"
