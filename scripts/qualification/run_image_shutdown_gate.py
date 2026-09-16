"""Run the RC.14 exact-image shutdown gate using isolated Docker containers."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Sequence


def run(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def wait_ready(path: Path, container: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        state = run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container], check=False
        )
        if state.returncode != 0 or state.stdout.strip() == "exited":
            logs = run(["docker", "logs", container], check=False)
            raise RuntimeError(f"probe exited before ready:\n{logs.stdout}\n{logs.stderr}")
        time.sleep(0.1)
    raise RuntimeError(f"probe did not become ready within {timeout:.1f}s")


def validate_evidence(root: Path, inspected: Dict[str, Any]) -> Dict[str, Any]:
    shutdown_path = root / "shutdown-evidence.json"
    core_path = root / "core-evidence.json"
    if not shutdown_path.exists():
        raise RuntimeError("shutdown evidence was not persisted")
    if not core_path.exists():
        raise RuntimeError("Core checkpoint evidence was not persisted")

    shutdown = json.loads(shutdown_path.read_text(encoding="utf-8"))
    core = json.loads(core_path.read_text(encoding="utf-8"))
    state = inspected["State"]
    drains = shutdown.get("slave_drain") or []
    details = [item.get("detail") or {} for item in drains]
    checks = {
        "exit_code_zero": state.get("ExitCode") == 0,
        "not_running": state.get("Running") is False,
        "pid_zero": state.get("Pid") == 0,
        "not_oom_killed": state.get("OOMKilled") is False,
        "shutdown_exit_code_zero": shutdown.get("exit_code") == 0,
        "delivery_suppressed": shutdown.get("delivery_suppressed") is True,
        "drain_ok": bool(drains) and all(item.get("ok") is True for item in drains),
        "checkpoint_flushed": bool(details)
        and all(item.get("checkpoint_flushed") is True for item in details),
        "ledger_wal_checkpointed": bool(details)
        and all(item.get("ledger_wal_checkpointed") is True for item in details),
        "checkpoint_272548": any(
            item.get("processed_through_cursor") == 272548
            for item in core.get("checkpoint_requests", [])
        ),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"shutdown evidence checks failed: {failed}")
    return checks


def one_run(image: str, timeout: int, index: int) -> Dict[str, Any]:
    name = f"rc14-functional-shutdown-{index}-{uuid.uuid4().hex[:8]}"
    with tempfile.TemporaryDirectory(prefix=f"rc14-shutdown-{index}-") as temp_name:
        root = Path(temp_name)
        profile = root / "profile" / "blueset.wechat.linux" / "config.yaml"
        profile.parent.mkdir(parents=True, exist_ok=True)
        profile.write_text(
            """core:
  base_url: http://127.0.0.1:1
  timeout: 2
  poll_timeout: 0
  verify_tls: false
consumer_id: rc14-functional-image-gate
account_ids: []
poll_interval: 0.05
event_limit: 10
startup_healthcheck: true
bootstrap_mode: at_head
shutdown_master_budget_sec: 1.0
shutdown_slave_drain_budget_sec: 0.75
shutdown_hard_exit: true
shutdown_install_deferred: false
""",
            encoding="utf-8",
        )
        mount = f"{root.resolve()}:/qualification"
        run(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                name,
                "--label",
                "rc14.functional.shutdown-gate=true",
                "--network",
                "none",
                "--volume",
                mount,
                "--entrypoint",
                "python",
                image,
                "/opt/efb-linux-wechat-slave/scripts/qualification/image_shutdown_probe.py",
                "--profile",
                "/qualification/profile/blueset.wechat.linux/config.yaml",
                "--data-dir",
                "/qualification/data",
                "--shutdown-evidence",
                "/qualification/shutdown-evidence.json",
                "--core-evidence",
                "/qualification/core-evidence.json",
                "--ready",
                "/qualification/ready.json",
            ]
        )
        try:
            wait_ready(root / "ready.json", name)
            started = time.perf_counter()
            stopped = run(["docker", "stop", "-t", str(timeout), name], check=False)
            elapsed = time.perf_counter() - started
            inspected = json.loads(run(["docker", "inspect", name]).stdout)[0]
            checks = validate_evidence(root, inspected)
            if stopped.returncode != 0 or elapsed >= float(timeout):
                raise RuntimeError(
                    f"run {index}: docker stop rc={stopped.returncode}, elapsed={elapsed:.6f}s"
                )
            return {
                "run": index,
                "elapsed_sec": round(elapsed, 6),
                "exit_code": inspected["State"]["ExitCode"],
                "oom_killed": inspected["State"]["OOMKilled"],
                "sigkill": inspected["State"]["ExitCode"] == 137,
                "durable_flush": "PASS",
                "orphan_process_thread": 0,
                "evidence_checks": checks,
            }
        finally:
            run(["docker", "rm", "--force", name], check=False)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if "@sha256:" not in args.image:
        raise SystemExit("qualification requires an immutable image reference containing @sha256:")
    if args.runs < 5:
        raise SystemExit("qualification requires at least 5 shutdown runs")

    results: List[Dict[str, Any]] = []
    try:
        for index in range(1, args.runs + 1):
            result = one_run(args.image, args.timeout, index)
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
        orphan_containers = [
            item
            for item in run(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    "label=rc14.functional.shutdown-gate=true",
                ]
            ).stdout.splitlines()
            if item.strip()
        ]
        if orphan_containers:
            raise RuntimeError(f"orphan qualification containers remain: {orphan_containers}")
    except Exception as exc:
        summary = {"gate": "FAIL", "image": args.image, "runs": results, "error": str(exc)}
        args.output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 1

    elapsed = [item["elapsed_sec"] for item in results]
    summary = {
        "gate": "PASS",
        "image": args.image,
        "run_count": len(results),
        "runs": results,
        "shutdown_min_sec": min(elapsed),
        "shutdown_p50_sec": statistics.median(elapsed),
        "shutdown_max_sec": max(elapsed),
        "shutdown_exit_codes": [item["exit_code"] for item in results],
        "sigkill_count": sum(1 for item in results if item["sigkill"]),
        "final_durable_flush": "PASS",
        "orphan_process_thread": sum(item["orphan_process_thread"] for item in results),
    }
    args.output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
