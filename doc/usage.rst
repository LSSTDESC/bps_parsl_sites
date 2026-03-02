Running at NERSC
----------------

A local copy of this package can be downloaded from github:

.. code-block:: bash

   $ git clone https://github.com/LSSTDESC/bps_parsl_sites.git

For running at NERSC, it's probably easiest to use the CVMFS distributions of the Rubin stack.  (Unfortunately, shifter or podman images won't work in general, since multinode processing using the slurm-based classes requires access to slurm commands, and those commands are not accessible from within a container.)  From a perlmutter login node (running bash), one can do

.. code-block:: bash

   $ source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2026_09/loadLSST-ext.bash
   (lsst-scipipe-12.0.0-exact) $ setup lsst_distrib
   (lsst-scipipe-12.0.0-exact) $ setup -r <path_to>/bps_parsl_sites -j

To configure ``bps`` to use parsl and the `TaskVine` site configuration in this package, a code block like the following can be added to the bps yaml config file:

.. code-block:: yaml

   wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
   #computeSite: local
   computeSite: task_vine

   parsl:
     log_level: WARN

  site:
    local:
      class: lsst.ctrl.bps.parsl.sites.Local
      cores: 8
      monitorEnable: true
      monitorFilename: runinfo/monitoring.db
    task_vine:
      class: bps_parsl_sites.SlurmTaskVine
      walltime: "00:30:00"
      qos: debug
      constraint: cpu
      exclusive: true
      nodes_per_block: 1
      provider_options:
        init_blocks: 1
        min_blocks: 0
        max_blocks: 5
      scheduler_options: |
        #SBATCH --module=cvmfs
      worker_options: "--memory=480000"  # total memory in MB
      monitorEnable: true
      monitorFilename: runinfo/monitoring.db

Here are notes on some of the entries in this configuration block:

**computeSite**
  This can be used to switch between the ``local`` config,
  which is useful for running locally on a single node, such as a laptop, and
  the ``task_vine`` config, which will submit jobs to slurm, typically from
  a Perlmutter login node.

**parsl.log_level**
  This controls the log-level of parsl output, which can
  be rather verbose.  This is separate from the logging control of the BPS
  software.  Unfortunately, there is also root-level parsl logging that
  can't be directly controlled without also affecting the bps log output.

**monitorFilename**
  This specifies the location of the sqlite3 monintoring.db file.  Enabling
  monitoring is useful for determining the state of the jobs in the workflow.

**site.local.cores**
  These are the number of cores to use on the local node
  for running jobs.  Note that per-task memory requests are ignored for running
  with the ``local`` ``computeSite`` config.

**site.task_vine**
  * **class**: This is the ``siteConfig`` class that uses parsl's ``TaskVineExecutor``
    for sheduling jobs on workers, based on the job resource requests.  There
    is also a ``SlurmWorkQueue`` class that uses the prior generation of executor,
    ``WorkQueueExcutor`` for managing the resources in a similar fashion.  Both ``TaskVine``
    and ``Work Queue`` are scheduler implementations developed by 
    [The Cooperative Computing Lab](https://ccl.cse.nd.edu/software).
  * **walltime**:  The wall time request for each batch submission. This must
    have the format ``hours:minutes:seconds`` since parsl tokenizes this string
    into three fields and will raise an error if it doesn't find all three.
  * **qos**:  This sets the job's quality-of-service.  Set this to ``debug``
    for running in the debug queue, to ``regular`` for the standard charge
    factor, etc..
  * **constraint**:  Set this to ``cpu``.
  * **exclusive**:  Whether exclusive nodes are used.  This should probably be
    set to ``true`` for running on Perlmutter.
  * **nodes_per_block**:  The number of batch nodes per block to request for
    running the workflow jobs.  Since all Rubin pipetasks currently 
    don't run across nodes, e.g., to support MPI communication, this can be
    set to ``1`` (or some appropriate number to avoid submitting too many batch jobs), 
    and the overall number of nodes should be controlled via the blocks settings
    in the `provider_options` below.
  * **provider_options**:  These options are passed to through parsl's 
    [`SlurmProvider`](https://github.com/Parsl/parsl/blob/2026.02.23/parsl/providers/slurm/slurm.py#L76)
    class as ``#SBATCH`` entries.  Each block will be submitted as a separate slurm
    batch job, so setting ``init_blocks=1`` will provide workers as soon as possible and
    pipetask jobs can start running.  The remaining requested blocks, as specified by
    `max_blocks`, will be submitted in order to provide workers for all pending tasks.
    As the number of tasks descrease or increae over the course of the overall workflow execution,
    blocks will be released or requested, respectively, within the ``min_blocks`` and ``max_blocks``
    bounds.   See the [parsl documentation](https://parsl.readthedocs.io/en/stable/userguide/configuration/providers/elasticity.html).
  * **scheduler_options**:  These are additional entries, not covered by the
    above parameters, to add to the sbatch submission scripts generated
    by parsl.
  * **worker_options**:  These are options to pass to the ``TaskVineExecutor``
    to tell it what resources should be made available for running pipeline jobs on a
    given batch node.  Perlmutter nodes have 512GB of RAM available, so passing
    ``--memory=480000`` reserves 32GB for non-pipeline processes running on the node.

Submitting a workflow
^^^^^^^^^^^^^^^^^^^^^

To submit a workflow, one uses the standard `bps submit` command.  Since parsl prints out
a lot of log messages to stderr, I will filter those out with grep and redirect
the screen output to a log file:

.. code-block:: bash

  $ (bps submit <bps_yaml_file> 2>&1 | grep -v ^parsl.process_loggers | grep -v ^monitoring_) &> bps_submission.log &
