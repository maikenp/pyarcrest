import argparse
import json
import logging
import os
import pathlib
import sys

from pyarcrest.arc import ARCJob, ARCRest

PROXYPATH = f"/tmp/x509up_u{os.getuid()}"


def main():
    parser = argparse.ArgumentParser("Execute ARC operations")
    parser.add_argument("cluster", type=str, help="hostname (with optional port) of the cluster")
    parser.add_argument("-P", "--proxy", type=str, default=PROXYPATH, help="path to proxy cert")
    parser.add_argument("-v", "--verbose", action="store_true", help="print debug output")

    subparsers = parser.add_subparsers(dest="command")

    version_parser = subparsers.add_parser(
        "version",
        help="get supported REST API versions",
    )

    info_parser = subparsers.add_parser(
        "info",
        help="get CE resource information"
    )

    jobs_parser = subparsers.add_parser(
        "jobs",
        help="execute operations on /jobs endpoint"
    )

    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs")
    jobs_list_parser = jobs_subparsers.add_parser(
        "list",
        help="get list of jobs",
    )
    jobs_info_parser = jobs_subparsers.add_parser(
        "info",
        help="get info for given jobs",
    )
    jobs_info_parser.add_argument("jobids", type=str, nargs='+', help="job IDs to fetch")
    jobs_clean_parser = jobs_subparsers.add_parser(
        "clean",
        help="clean given jobs",
    )
    jobs_clean_parser.add_argument("jobids", type=str, nargs='+', help="job IDs to clean")
    jobs_submit_parser = jobs_subparsers.add_parser(
        "submit",
        help="submit given job descriptions",
    )
    jobs_submit_parser.add_argument("jobdescs", type=pathlib.Path, nargs='+', help="job descs to submit")
    jobs_submit_parser.add_argument("--queue", type=str, help="queue to submit to")

    args = parser.parse_args()

    kwargs = {}
    if args.verbose:
        logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        kwargs["logger"] = logging.getLogger()
    arcrest = ARCRest.getClient(url=args.cluster, proxypath=args.proxy, **kwargs)

    if args.command == "jobs" and args.jobs == "list":
        status, text = arcrest._requestJSON("GET", f"{arcrest.apiPath}/jobs")
        if status != 200:
            print(f"ARC jobs list error: {status} {text}")
        else:
            try:
                print(json.dumps(json.loads(text), indent=4))
            except json.JSONDecodeError as exc:
                if exc.doc != "":
                    raise

    elif args.command == "jobs" and args.jobs in ("info", "clean"):
        tomanage = [{"id": arcid} for arcid in args.jobids]
        if not tomanage:
            return

        jsonData = {}
        if len(tomanage) == 1:
            jsonData["job"] = tomanage[0]
        else:
            jsonData["job"] = tomanage

        status, text = arcrest._requestJSON(
            "POST",
            f"{arcrest.apiPath}/jobs?action={args.jobs}",
            data=json.dumps(jsonData).encode(),
            headers={"Content-type": "application/json"},
        )
        if status != 201:
            print(f"ARC jobs operation error: {status} {text}")
        else:
            print(json.dumps(json.loads(text), indent=4))

    elif args.command == "jobs" and args.jobs == "submit":
        jobs = []
        for desc in args.jobdescs:
            with desc.open() as f:
                jobs.append(ARCJob(descstr=f.read()))
        arcrest.submitJobs(args.queue, jobs)
        for job in jobs:
            print(job.id)

    elif args.command == "version":
        status, text = arcrest._requestJSON("GET", f"{arcrest.apiBase}/rest")
        if status != 200:
            print(f"ARC CE REST API versions error: {status} {text}")
        else:
            print(json.dumps(json.loads(text), indent=4))

    elif args.command == "info":
        status, text = arcrest._requestJSON("GET", f"{arcrest.apiPath}/info")
        if status != 200:
            print(f"ARC CE info error: {status} {text}")
        else:
            print(json.dumps(json.loads(text), indent=4))
