"""Unit tests for bps_parsl_sites/task_vine.py

Run with:
    python -m pytest tests/test_task_vine.py -v
"""

import unittest
from unittest.mock import MagicMock, patch

from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig, TaskVineFactoryConfig
from parsl.providers.base import ExecutionProvider
from lsst.ctrl.bps.parsl.site import SiteConfig

from bps_parsl_sites.task_vine import TaskVine, SlurmTaskVine, LocalTaskVine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_site_instance(cls):
    """Instantiate a site class bypassing the real BPS config requirement."""
    with patch.object(SiteConfig, 'get_site_subconfig', return_value=MagicMock()):
        return cls(MagicMock())


# ---------------------------------------------------------------------------
# Tests: TaskVine base class
# ---------------------------------------------------------------------------

class TestTaskVineInit(unittest.TestCase):

    def test_resource_list_injected(self):
        """__init__ must inject the required resource_list into kwargs."""
        site = _make_site_instance(SlurmTaskVine)
        self.assertIn("memory", site.resource_list)
        self.assertIn("cores", site.resource_list)
        self.assertIn("disk", site.resource_list)
        self.assertIn("running_time_min", site.resource_list)
        self.assertIn("priority", site.resource_list)

    def test_resource_list_length(self):
        site = _make_site_instance(SlurmTaskVine)
        self.assertEqual(len(site.resource_list), 5)

    def test_is_site_config_subclass(self):
        self.assertTrue(issubclass(TaskVine, SiteConfig))

    def test_slurm_is_task_vine_subclass(self):
        self.assertTrue(issubclass(SlurmTaskVine, TaskVine))

    def test_local_is_task_vine_subclass(self):
        self.assertTrue(issubclass(LocalTaskVine, TaskVine))


# ---------------------------------------------------------------------------
# Tests: TaskVine.make_executor
# ---------------------------------------------------------------------------

class TestMakeExecutor(unittest.TestCase):

    def setUp(self):
        self.site = _make_site_instance(SlurmTaskVine)
        self.provider = MagicMock(spec=ExecutionProvider)
        patcher = patch(
            'bps_parsl_sites.task_vine.get_bps_config_value',
            side_effect=lambda site, key, typ, default: default,
        )
        self.mock_gcbv = patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_task_vine_executor(self):
        ex = self.site.make_executor("my_label", self.provider)
        self.assertIsInstance(ex, TaskVineExecutor)

    def test_label_passed_through(self):
        ex = self.site.make_executor("custom_label", self.provider)
        self.assertEqual(ex.label, "custom_label")

    def test_worker_launch_method_is_provider(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.worker_launch_method, "provider")

    def test_provider_passed_through(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertIs(ex.provider, self.provider)

    def test_manager_config_is_set(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertIsInstance(ex.manager_config, TaskVineManagerConfig)

    def test_factory_config_is_set(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertIsInstance(ex.factory_config, TaskVineFactoryConfig)

    def test_manager_config_address_from_get_address(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.manager_config.address, self.site.get_address())

    def test_manager_config_port_is_positive_int(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertIsInstance(ex.manager_config.port, int)
        self.assertGreater(ex.manager_config.port, 0)

    def test_manager_config_port_from_get_free_port(self):
        with patch('bps_parsl_sites.task_vine.get_free_port', return_value=54321):
            ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.manager_config.port, 54321)

    def test_default_max_retries_is_1(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.manager_config.max_retries, 1)

    def test_custom_tv_max_retries(self):
        ex = self.site.make_executor("lbl", self.provider, tv_max_retries=3)
        self.assertEqual(ex.manager_config.max_retries, 3)

    def test_tv_max_retries_none_allowed(self):
        ex = self.site.make_executor("lbl", self.provider, tv_max_retries=None)
        self.assertIsNone(ex.manager_config.max_retries)

    def test_default_worker_options_is_none(self):
        ex = self.site.make_executor("lbl", self.provider)
        self.assertIsNone(ex.factory_config.worker_options)

    def test_custom_worker_options_passed(self):
        ex = self.site.make_executor("lbl", self.provider, worker_options="--memory=90000")
        self.assertEqual(ex.factory_config.worker_options, "--memory=90000")

    def test_worker_options_override_from_bps_config(self):
        """get_bps_config_value can override the default worker_options."""
        self.mock_gcbv.side_effect = (
            lambda site, key, typ, default: "--cores=4" if key == "worker_options" else default
        )
        ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.factory_config.worker_options, "--cores=4")

    def test_max_retries_override_from_bps_config(self):
        """get_bps_config_value can override the default tv_max_retries."""
        self.mock_gcbv.side_effect = (
            lambda site, key, typ, default: 5 if key == "tv_max_retries" else default
        )
        ex = self.site.make_executor("lbl", self.provider)
        self.assertEqual(ex.manager_config.max_retries, 5)

    def test_get_bps_config_value_called_for_worker_options(self):
        self.site.make_executor("lbl", self.provider, worker_options="--x")
        keys = [c.args[1] for c in self.mock_gcbv.call_args_list]
        self.assertIn("worker_options", keys)

    def test_get_bps_config_value_called_for_tv_max_retries(self):
        self.site.make_executor("lbl", self.provider)
        keys = [c.args[1] for c in self.mock_gcbv.call_args_list]
        self.assertIn("tv_max_retries", keys)


# ---------------------------------------------------------------------------
# Tests: SlurmTaskVine
# ---------------------------------------------------------------------------

class TestSlurmTaskVine(unittest.TestCase):

    def setUp(self):
        self.site = _make_site_instance(SlurmTaskVine)
        slurm_patcher = patch(
            'bps_parsl_sites.task_vine.get_slurm_provider',
            return_value=MagicMock(spec=ExecutionProvider),
        )
        gcbv_patcher = patch(
            'bps_parsl_sites.task_vine.get_bps_config_value',
            side_effect=lambda site, key, typ, default: default,
        )
        self.mock_gsp = slurm_patcher.start()
        self.mock_gcbv = gcbv_patcher.start()
        self.addCleanup(slurm_patcher.stop)
        self.addCleanup(gcbv_patcher.stop)

    def test_get_executors_returns_list(self):
        result = self.site.get_executors()
        self.assertIsInstance(result, list)

    def test_get_executors_returns_one_executor(self):
        result = self.site.get_executors()
        self.assertEqual(len(result), 1)

    def test_get_executors_contains_task_vine_executor(self):
        result = self.site.get_executors()
        self.assertIsInstance(result[0], TaskVineExecutor)

    def test_executor_label_is_slurm_task_vine(self):
        result = self.site.get_executors()
        self.assertEqual(result[0].label, "slurm_task_vine")

    def test_get_slurm_provider_called(self):
        self.site.get_executors()
        self.mock_gsp.assert_called_once_with(self.site)

    def test_select_executor_returns_string(self):
        job = MagicMock()
        result = self.site.select_executor(job)
        self.assertIsInstance(result, str)

    def test_select_executor_returns_slurm_task_vine(self):
        job = MagicMock()
        self.assertEqual(self.site.select_executor(job), "slurm_task_vine")

    def test_select_executor_label_matches_get_executors_label(self):
        job = MagicMock()
        executor_labels = [ex.label for ex in self.site.get_executors()]
        self.assertIn(self.site.select_executor(job), executor_labels)

    def test_select_executor_independent_of_job(self):
        job1, job2 = MagicMock(), MagicMock()
        self.assertEqual(
            self.site.select_executor(job1),
            self.site.select_executor(job2),
        )


# ---------------------------------------------------------------------------
# Tests: LocalTaskVine
# ---------------------------------------------------------------------------

class TestLocalTaskVine(unittest.TestCase):

    def setUp(self):
        self.site = _make_site_instance(LocalTaskVine)
        local_patcher = patch(
            'bps_parsl_sites.task_vine.get_local_provider',
            return_value=MagicMock(spec=ExecutionProvider),
        )
        gcbv_patcher = patch(
            'bps_parsl_sites.task_vine.get_bps_config_value',
            side_effect=lambda site, key, typ, default: default,
        )
        self.mock_glp = local_patcher.start()
        self.mock_gcbv = gcbv_patcher.start()
        self.addCleanup(local_patcher.stop)
        self.addCleanup(gcbv_patcher.stop)

    def test_get_executors_returns_list(self):
        result = self.site.get_executors()
        self.assertIsInstance(result, list)

    def test_get_executors_returns_one_executor(self):
        result = self.site.get_executors()
        self.assertEqual(len(result), 1)

    def test_get_executors_contains_task_vine_executor(self):
        result = self.site.get_executors()
        self.assertIsInstance(result[0], TaskVineExecutor)

    def test_executor_label_is_local_task_vine(self):
        result = self.site.get_executors()
        self.assertEqual(result[0].label, "local_task_vine")

    def test_get_local_provider_called(self):
        self.site.get_executors()
        self.mock_glp.assert_called_once_with(self.site)

    def test_select_executor_returns_string(self):
        job = MagicMock()
        result = self.site.select_executor(job)
        self.assertIsInstance(result, str)

    def test_select_executor_returns_local_task_vine(self):
        job = MagicMock()
        self.assertEqual(self.site.select_executor(job), "local_task_vine")

    def test_select_executor_label_matches_get_executors_label(self):
        job = MagicMock()
        executor_labels = [ex.label for ex in self.site.get_executors()]
        self.assertIn(self.site.select_executor(job), executor_labels)

    def test_select_executor_independent_of_job(self):
        job1, job2 = MagicMock(), MagicMock()
        self.assertEqual(
            self.site.select_executor(job1),
            self.site.select_executor(job2),
        )


# ---------------------------------------------------------------------------
# Tests: module-level __all__
# ---------------------------------------------------------------------------

class TestModuleAll(unittest.TestCase):

    def test_all_is_defined(self):
        import bps_parsl_sites.task_vine as m
        self.assertTrue(hasattr(m, "__all__"))

    def test_all_contains_slurm_task_vine(self):
        import bps_parsl_sites.task_vine as m
        self.assertIn("SlurmTaskVine", m.__all__)

    def test_all_contains_local_task_vine(self):
        import bps_parsl_sites.task_vine as m
        self.assertIn("LocalTaskVine", m.__all__)

    def test_task_vine_not_in_all(self):
        """Base class TaskVine should not be exported."""
        import bps_parsl_sites.task_vine as m
        self.assertNotIn("TaskVine", m.__all__)


if __name__ == "__main__":
    unittest.main()
