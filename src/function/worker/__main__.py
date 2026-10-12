import argparse
import asyncio
from fractions import Fraction

from cp.config import get_settings
from cp.logging_config import configure_application_logging
from function.worker.simulator import WorkerOptions, WorkerSimulator


def parse_failure_rate(value: str) -> float:
    try:
        rate = float(Fraction(value))
    except (ValueError, ZeroDivisionError) as exc:
        raise argparse.ArgumentTypeError("fail rate must be a number or ratio like 1/10") from exc
    if not 0 <= rate <= 1:
        raise argparse.ArgumentTypeError("fail rate must be between 0 and 1")
    return rate


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Tenant provisioning worker simulator")
    parser.add_argument(
        "--fail-rate",
        type=parse_failure_rate,
        default=settings.worker_fail_rate,
        help="target probability a task ultimately fails; accepts values like 0.1 or 1/10",
    )
    parser.add_argument("--min-delay-ms", type=int, default=settings.worker_min_delay_ms)
    parser.add_argument("--max-delay-ms", type=int, default=settings.worker_max_delay_ms)
    parser.add_argument("--max-attempts", type=int, default=settings.worker_max_attempts)
    parser.add_argument(
        "--retry-backoff-ms",
        type=int,
        default=settings.worker_retry_backoff_ms,
    )
    parser.add_argument("--concurrency", type=int, default=settings.worker_concurrency)
    parser.add_argument("--duplicate-rate", type=float, default=settings.worker_duplicate_rate)
    parser.add_argument("--seed", type=int, default=settings.worker_seed)
    parser.add_argument("--queue-name", default="cp.tasks.worker")
    return parser


async def run_worker(options: WorkerOptions) -> None:
    stop_event = asyncio.Event()
    await WorkerSimulator(options).run(stop_event)


def main() -> None:
    configure_application_logging()
    args = build_parser().parse_args()
    options = WorkerOptions(
        fail_rate=args.fail_rate,
        min_delay_ms=args.min_delay_ms,
        max_delay_ms=args.max_delay_ms,
        max_attempts=args.max_attempts,
        retry_backoff_ms=args.retry_backoff_ms,
        concurrency=args.concurrency,
        duplicate_rate=args.duplicate_rate,
        seed=args.seed,
        queue_name=args.queue_name,
    )
    try:
        asyncio.run(run_worker(options))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
