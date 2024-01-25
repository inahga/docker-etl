import asyncio
import click
import datetime
import json
import math
import os
import re
import subprocess
import time
import typing

# from google.cloud import bigquery
import requests

LEADER = "https://staging-dap-07.api.divviup.org/"
CMD = f"/home/inahga/Projects/docker-etl/jobs/dap-collector/dap_collector/collect --task-id {{task_id}} --leader {LEADER} --vdaf {{vdaf}} --batch-interval-start {{timestamp}} --batch-interval-duration {{duration}} --collector-credential-file ~/Projects/credentials.json"
INTERVAL_LENGTH = 300


def read_tasks():
    return [
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
        {
            "task_id": "5gw0C5DByr09JgwjY5f85YDy5MmS9By34YOOJBN0m9w",
            "vdaf": "count",
        },
    ]


def toh(timestamp):
    """Turn a timestamp into a datetime object which prints human readably."""
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


async def collect_once(task, timestamp, duration):
    """Runs collection for a single time interval.

    This uses the Janus collect binary. The result is formatted to fit the BQ table.
    """
    collection_time = str(datetime.datetime.now(datetime.timezone.utc).timestamp())
    print(f"{collection_time} Collecting {toh(timestamp)} - {toh(timestamp+duration)}")

    # Prepare output
    res = {}
    res["task_id"] = task["task_id"]

    res["collection_time"] = collection_time
    res["slot_start"] = timestamp

    # Convert VDAF description to string for command line use
    # vdaf_args = ""
    # for k, v in task["vdaf_args_structured"].items():
    #     vdaf_args += f" --{k} {v}"

    cmd = CMD.format(
        timestamp=timestamp,
        duration=duration,
        task_id=task["task_id"],
        vdaf=task["vdaf"],
    )

    # How long an individual collection can take before it is killed.
    timeout = 100
    start_counter = time.perf_counter()
    try:
        proc = await asyncio.wait_for(
            asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            ),
            timeout,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        stdout = stdout.decode()
        stderr = stderr.decode()
    except asyncio.exceptions.TimeoutError:
        res["collection_duration"] = time.perf_counter() - start_counter
        res["error"] = f"TIMEOUT"
        return res
    res["collection_duration"] = time.perf_counter() - start_counter

    print(stdout)
    print(stderr)

    # Parse the output of the collect binary
    if proc.returncode == 1:
        if (
            stderr
            == "Error: HTTP response status 400 Bad Request - The number of reports included in the batch is invalid.\n"
        ):
            res["error"] = "BATCH TOO SMALL"
        else:
            res["error"] = f"UNHANDLED ERROR: {stderr }"
    else:
        for line in stdout.splitlines():
            if line.startswith("Aggregation result:"):
                if task["vdaf"] in ["countvec", "sumvec"]:
                    entries = line[21:-1]
                    entries = list(map(int, entries.split(",")))
                    res["value"] = entries
                elif task["vdaf"] in ["sum", "count"]:
                    s = int(line[20:])
                    res["value"] = [s]
                else:
                    raise RuntimeError(f"Unknown VDAF: {task['vdaf']}")
            elif line.startswith("Number of reports:"):
                res["report_count"] = int(line.split()[-1].strip())
            elif (
                line.startswith("Interval start:")
                or line.startswith("Interval end:")
                or line.startswith("Interval length:")
            ):
                # irrelevant since we are using time interval queries
                continue
            else:
                print(f"UNHANDLED OUTPUT LINE: {line}")
                raise NotImplementedError

    print(res)
    return res


async def process_queue(q: asyncio.Queue, results: list):
    """Worker for parallelism. Processes items from the qeueu until it is empty."""
    while not q.empty():
        job = q.get_nowait()
        res = await collect_once(*job)
        results.append(res)


async def collect_many(
    task, time_from, time_until, interval_length,
):
    """Collects data for a given time interval.

    Creates a configurable amount of workers which process jobs from a queue
    for parallelism.
    """
    time_from = int(time_from.timestamp())
    time_until = int(time_until.timestamp())
    start = math.ceil(time_from // interval_length) * interval_length
    jobs = asyncio.Queue(288)
    results = []
    while start + interval_length <= time_until:
        await jobs.put((task, start, interval_length))
        start += interval_length
    workers = []
    for _ in range(10):
        workers.append(process_queue(jobs, results))
    await asyncio.gather(*workers)

    return results


async def collect_task(task, date):
    """Collects data for the given task and the given day."""
    start = datetime.datetime.fromisoformat(date)
    start = start.replace(tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(days=1)

    results = await collect_many(
        task, start, end, INTERVAL_LENGTH
    )

    return results

@click.command()
@click.option(
    "--date",
    help="Date at which the backfill will start, going backwards (YYYY-MM-DD)",
    required=True,
)
def main(date):
    for task in read_tasks():
        print(f"Now processing task: {task['task_id']}")
        results = asyncio.run(collect_task(task, date))


if __name__ == "__main__":
    main()
