"""TaskForge Dedicated Process Task Runner.

Executes a single task in an isolated OS process with safe argument parsing.
Invoked by execute_task_process_isolated() via:
    python -m taskforge.workers.process_runner --task-id <id> --worker-id <id> --timeout <sec> --lease-token <tok>
"""

import argparse
import sys

from taskforge.core.logging import get_logger
from taskforge.workers.execution_engine import execute_task

logger = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="TaskForge Dedicated Process Task Runner")
    parser.add_argument("--task-id", type=str, required=True, help="Task UUID to execute")
    parser.add_argument("--worker-id", type=str, required=True, help="Worker identifier")
    parser.add_argument("--timeout", type=int, default=60, help="Execution timeout in seconds")
    parser.add_argument("--lease-token", type=str, default=None, help="Optional lease token")

    args = parser.parse_args()

    try:
        success = execute_task(
            task_id=args.task_id,
            worker_id=args.worker_id,
            timeout_seconds=args.timeout,
            lease_token=args.lease_token,
        )
        return 0 if success else 1
    except PermissionError as perm_err:
        logger.warning(
            "process_runner_fencing_violation",
            task_id=args.task_id,
            worker_id=args.worker_id,
            error=str(perm_err),
        )
        return 3
    except Exception as exc:
        logger.error(
            "process_runner_fatal_error",
            task_id=args.task_id,
            worker_id=args.worker_id,
            error=str(exc),
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())

