"""Prepare/build an ADO-backed Iguazio worker; never submit a job or install a schedule."""
from __future__ import annotations

import argparse
import os
from dataclasses import fields
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse


def configuration(environ=None):
    if __package__:
        from .attribute_health.config import Settings
        from .attribute_health.readiness import READINESS_DEFAULTS
    else:
        from attribute_health.config import Settings
        from attribute_health.readiness import READINESS_DEFAULTS
    env = os.environ if environ is None else environ
    branch = env.get("IQUAZIO_SOURCE_BRANCH", "")
    source = env.get("OPTIC_HEALTH_ADO_SOURCE_URL", "")
    if not source and branch:
        source = "https://dev.azure.com/vistraretail/iguazio/_git/mts-marketing-tech#refs/heads/" + branch
    job_path = env.get("OPTIC_HEALTH_JOB_PATH", "")
    browser_path = env.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/playwright-browsers")
    allowed = {"OPTIC_HEALTH_" + x.name.upper() for x in fields(Settings)}
    allowed.update("OPTIC_HEALTH_" + name.upper() for name in READINESS_DEFAULTS)
    allowed.update({"OPTIC_HEALTH_" + name + "_TABLE" for name in (
        "INTEGRATED", "DETAILS", "PROMPTS", "ATTRIBUTES", "RESULTS", "ANALYSIS", "CONTENT", "JOBS")})
    allowed.add("OPTIC_HEALTH_BROWSER_CHANNEL")
    runtime = {name: value for name, value in env.items() if name in allowed}
    defaults = Settings()
    runtime.update({"OPTIC_HEALTH_" + name.upper(): env.get(
        "OPTIC_HEALTH_" + name.upper(), str(getattr(defaults, name, default)))
        for name, default in READINESS_DEFAULTS.items()})
    runtime.update(OPTIC_HEALTH_AI_ENABLED="0", OPTIC_HEALTH_JUDGE_MODEL="gpt-6.1-sol",
                   OPTIC_HEALTH_REVIEWER_MODEL=env.get("OPTIC_HEALTH_REVIEWER_MODEL", "gpt-6.1-sol"),
                   PLAYWRIGHT_BROWSERS_PATH=browser_path, PYTHONUNBUFFERED="1")
    for name in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
                 "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE", "ODIN_GATEWAY_BASE_URL", "ODIN_API_BASE_URL"):
        if env.get(name):
            runtime[name] = env[name]
    requirements = [line.strip() for line in (Path(__file__).parent / "requirements-health.txt").read_text(
        encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#")]
    return {"project": env.get("OPTIC_HEALTH_PROJECT", "marketing-tech"),
            "job": env.get("OPTIC_HEALTH_JOB", "optic-attribute-monitor-job"),
            "racf_id": env.get("OPTIC_HEALTH_RACF_ID", ""), "source": source, "job_path": job_path,
            "cpu": env.get("OPTIC_HEALTH_JOB_CPU", "2000m"),
            "memory": env.get("OPTIC_HEALTH_JOB_MEMORY", "8Gi"),
            "image": "mlrun/mlrun:1.10.1",
            "requirements": ["--extra-index-url=https://pypi.eap.tceh.net/simple",
                             "mlrun==1.10.1", "vistra-dsml==1.10.1.6", *requirements],
            "commands": [f"PLAYWRIGHT_BROWSERS_PATH={browser_path} python -m playwright install --with-deps chromium"],
            "env": runtime, "with_repo": True, "autorun": False, "schedule": None,
            "shared_state_confirmed": env.get("OPTIC_HEALTH_SHARED_STATE_CONFIRMED") == "1"}


def _validate(config):
    source = urlparse(config["source"])
    if source.scheme != "https" or source.netloc != "dev.azure.com" or not source.fragment.startswith("refs/heads/"):
        raise ValueError("Configure an explicit HTTPS ADO repo source and branch before setup.")
    path = PurePosixPath(config["job_path"].replace("\\", "/"))
    if not config["job_path"] or path.is_absolute() or ".." in path.parts or path.suffix != ".py":
        raise ValueError("Configure the confirmed ADO-relative OPTIC_HEALTH_JOB_PATH.")
    if not config["racf_id"]:
        raise ValueError("OPTIC_HEALTH_RACF_ID is required.")
    if not config["env"].get("OPTIC_HEALTH_STATE_DIR") or not config["shared_state_confirmed"]:
        raise ValueError("Confirm a durable shared mounted state directory before deployment.")
    browser = config["env"]["PLAYWRIGHT_BROWSERS_PATH"]
    if not browser.startswith("/") or any(x in browser for x in (" ", "'", '"', ";", "$", "`", "\n")):
        raise ValueError("Invalid deployment browser path.")


def _apply(fn, config):
    for name, value in config["env"].items():
        fn.set_env(name, value)
    fn.with_requests(cpu=config["cpu"], mem=config["memory"])
    fn.with_limits(cpu=config["cpu"], mem=config["memory"])
    fn.spec.state_thresholds = {"pending_scheduled": "15m", "pending_not_scheduled": "15m",
                               "image_pull_backoff": "15m", "executing": "60m"}
    return fn


def main(*, execute=False, config=None, vdsml_factory=None, mlrun_module=None):
    config = config or configuration()
    if not execute:
        return dict(config, status="plan only; no remote operations")
    _validate(config)
    if vdsml_factory is None:
        from vistra_dsml import VistraDsml
        vdsml_factory = VistraDsml
    if mlrun_module is None:
        import mlrun
        mlrun_module = mlrun
    vdsml = vdsml_factory(racf_id=config["racf_id"])
    vdsml.get_project(config["project"])
    vdsml.source = config["source"]
    vdsml.create_job(name=config["job"], python_file=config["job_path"], python_function="main",
                     project_name=config["project"], requirements=config["requirements"],
                     build_commands=config["commands"], cpu=config["cpu"], memory=config["memory"],
                     image=config["image"], with_repo=True)
    project = mlrun_module.get_or_create_project(config["project"], context=str(Path(__file__).parent))
    fn = _apply(project.get_function(config["job"], ignore_cache=True), config)
    fn.spec.build.base_image = config["image"]
    fn.spec.build.requirements = config["requirements"]
    # The wrapper's build_commands alone does not reliably persist MLRun build commands.
    fn.spec.build.with_commands(config["commands"])
    project.set_function(fn)
    project.save()
    build = project.build_function(fn)
    image = (getattr(build, "outputs", None) or {}).get("image")
    if getattr(build, "ready", False) is not True or not image:
        raise RuntimeError("Worker image build failed; no ready revision was published.")
    uri = f"db://{config['project']}/{config['job']}:latest"
    built = _apply(mlrun_module.import_function(uri), config)
    if built.status.state != "ready":
        raise RuntimeError("Built worker is not ready.")
    built.spec.image = image
    built.spec.build.image = image
    built.spec.build.requirements = []
    built.spec.build.commands = []
    if built.requires_build():
        raise RuntimeError("Built worker still requires a build.")
    built.save(tag="latest", versioned=True, refresh=True)
    verified = mlrun_module.import_function(uri)
    if not verified.metadata.uid or verified.metadata.uid == "unversioned-latest" or (
            verified.status.state != "ready" or verified.requires_build() or verified.spec.image != image):
        raise RuntimeError("Versioned ready worker failed readback verification.")
    actual_env = {x["name"]: x.get("value") for x in verified.to_dict()["spec"]["env"]}
    if any(actual_env.get(k) != v for k, v in config["env"].items()):
        raise RuntimeError("Worker environment did not persist.")
    for kind in ("requests", "limits"):
        actual = verified.spec.resources.get(kind, {})
        if actual.get("cpu") != config["cpu"] or actual.get("memory") != config["memory"]:
            raise RuntimeError("Worker resources did not persist.")
    project.set_function(uri, name=config["job"])
    project.save()
    return {"status": "ready", "function": uri, "image": image, "autorun": False, "schedule": None}


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument("--execute", action="store_true", help="Explicitly perform authorized setup/build")
    result = main(execute=argument_parser.parse_args().execute)
    print(result["status"])
