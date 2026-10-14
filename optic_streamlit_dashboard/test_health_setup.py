from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import OPTIC_ATTRIBUTE_MONITOR_Setup as setup


class Function:
    def __init__(self):
        self.env = {}
        self.spec = SimpleNamespace(resources={}, build=SimpleNamespace(
            with_commands=Mock(), commands=[], requirements=[]), image="")
        self.status = SimpleNamespace(state="ready")
        self.metadata = SimpleNamespace(uid="versioned-ready")
        self.save = Mock()

    def set_env(self, name, value):
        self.env[name] = value

    def with_requests(self, cpu, mem):
        self.spec.resources["requests"] = {"cpu": cpu, "memory": mem}

    def with_limits(self, cpu, mem):
        self.spec.resources["limits"] = {"cpu": cpu, "memory": mem}

    def requires_build(self):
        return False

    def to_dict(self):
        return {"spec": {"env": [{"name": k, "value": v} for k, v in self.env.items()]}}


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.env = {"OPTIC_HEALTH_RACF_ID": "mock-racf", "IQUAZIO_SOURCE_BRANCH": "approved-handoff",
                    "OPTIC_HEALTH_JOB_PATH": "optic_streamlit_dashboard/OPTIC_ATTRIBUTE_MONITOR.py",
                    "OPTIC_HEALTH_STATE_DIR": "/data/authorized-health-state",
                    "OPTIC_HEALTH_SHARED_STATE_CONFIRMED": "1"}
        self.config = setup.configuration(self.env)
        self.fn = Function()
        self.factory = Mock()
        self.mlrun = Mock()
        self.project = self.mlrun.get_or_create_project.return_value
        self.project.get_function.return_value = self.fn
        self.project.build_function.return_value = SimpleNamespace(ready=True, outputs={"image": "built-worker"})
        self.mlrun.import_function.return_value = self.fn

    def execute(self):
        return setup.main(execute=True, config=self.config, vdsml_factory=self.factory, mlrun_module=self.mlrun)

    def test_default_plan_only_no_remote_import_or_calls(self):
        result = setup.main(config=self.config, vdsml_factory=self.factory, mlrun_module=self.mlrun)
        self.assertIn("plan only", result["status"])
        self.factory.assert_not_called()
        self.mlrun.get_or_create_project.assert_not_called()

    def test_explicit_build_persisted_env_resources_with_repo_no_autorun(self):
        result = self.execute()
        create = self.factory.return_value.create_job.call_args.kwargs
        self.assertTrue(create["with_repo"])
        self.assertEqual(self.factory.return_value.source, self.config["source"])
        self.project.build_function.assert_called_once_with(self.fn)
        self.assertEqual(self.fn.env["OPTIC_HEALTH_AI_ENABLED"], "0")
        self.assertEqual(self.fn.env["PLAYWRIGHT_BROWSERS_PATH"], "/opt/playwright-browsers")
        self.assertEqual(self.fn.spec.resources["requests"], {"cpu": "2000m", "memory": "8Gi"})
        self.assertEqual(self.fn.spec.resources["limits"], self.fn.spec.resources["requests"])
        self.fn.spec.build.with_commands.assert_called_once_with(self.config["commands"])
        self.fn.save.assert_called_once_with(tag="latest", versioned=True, refresh=True)
        self.factory.return_value.run_job.assert_not_called()
        self.assertFalse(result["autorun"])
        self.assertIsNone(result["schedule"])

    def test_missing_ado_branch_or_relative_job_path_fails_before_remote(self):
        for changed in (dict(self.config, source=""), dict(self.config, job_path="../worker.py"),
                        dict(self.config, job_path="/absolute.py")):
            with self.assertRaises(ValueError):
                setup.main(execute=True, config=changed, vdsml_factory=self.factory, mlrun_module=self.mlrun)
        self.factory.assert_not_called()

    def test_durable_state_acknowledgment_required(self):
        self.config["shared_state_confirmed"] = False
        with self.assertRaises(ValueError):
            self.execute()
        self.factory.assert_not_called()

    def test_browser_shell_injection_rejected(self):
        self.config["env"]["PLAYWRIGHT_BROWSERS_PATH"] = "/opt/browser;echo-secret"
        with self.assertRaises(ValueError):
            self.execute()
        self.factory.assert_not_called()

    def test_failed_build_never_publishes_db_reference(self):
        self.project.build_function.return_value = SimpleNamespace(ready=False, outputs={})
        with self.assertRaises(RuntimeError):
            self.execute()
        self.mlrun.import_function.assert_not_called()

    def test_unversioned_or_resource_readback_fails_closed(self):
        self.fn.metadata.uid = "unversioned-latest"
        with self.assertRaises(RuntimeError):
            self.execute()
        self.assertFalse(any(isinstance(x.args[0], str) and x.args[0].startswith("db://")
                             for x in self.project.set_function.call_args_list))

    def test_secrets_never_persisted_in_env(self):
        config = setup.configuration(dict(self.env, ODIN_API_TOKEN="secret", SNOWFLAKE_PRIVATE_KEY_PEM="secret",
                                           OPTIC_HEALTH_ODIN_API_TOKEN="secret"))
        self.assertNotIn("ODIN_API_TOKEN", config["env"])
        self.assertNotIn("SNOWFLAKE_PRIVATE_KEY_PEM", config["env"])
        self.assertNotIn("OPTIC_HEALTH_ODIN_API_TOKEN", config["env"])

    def test_ai_environment_enabled_is_still_deployed_disabled(self):
        config = setup.configuration(dict(self.env, OPTIC_HEALTH_AI_ENABLED="1"))
        self.assertEqual(config["env"]["OPTIC_HEALTH_AI_ENABLED"], "0")

    def test_all_source_readiness_settings_persist_explicit_defaults(self):
        from attribute_health.readiness import READINESS_DEFAULTS
        for name, value in READINESS_DEFAULTS.items():
            self.assertEqual(self.config["env"]["OPTIC_HEALTH_" + name.upper()], str(value))
        self.execute()
        self.assertEqual(self.fn.env["OPTIC_HEALTH_SOURCE_LAG_DAYS"], "1")

    def test_source_lag_and_new_readiness_overrides_pass_through_without_enabling_ai(self):
        config = setup.configuration(dict(self.env, OPTIC_HEALTH_SOURCE_LAG_DAYS="3",
            OPTIC_HEALTH_READINESS_VOLUME_FRACTION="0.65", OPTIC_HEALTH_READINESS_ATTRIBUTE_READY_FRACTION="0.95"))
        self.assertEqual(config["env"]["OPTIC_HEALTH_SOURCE_LAG_DAYS"], "3")
        self.assertEqual(config["env"]["OPTIC_HEALTH_READINESS_VOLUME_FRACTION"], "0.65")
        self.assertEqual(config["env"]["OPTIC_HEALTH_READINESS_ATTRIBUTE_READY_FRACTION"], "0.95")
        self.assertEqual(config["env"]["OPTIC_HEALTH_AI_ENABLED"], "0")
