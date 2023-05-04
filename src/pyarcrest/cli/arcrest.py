import argparse
import json
import os
import pathlib
import ssl
from http.client import HTTPSConnection
from urllib.parse import urlparse

PROXYPATH = f"/tmp/x509up_u{os.getuid()}"


def main():
    parser = argparse.ArgumentParser("Execute ARC operations")
    parser.add_argument("cluster", type=str, help="hostname (with optional port) of the cluster")
    parser.add_argument("-P", "--proxy", type=str, default=PROXYPATH, help="path to proxy cert")

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

    args = parser.parse_args()

    url = urlparse(args.cluster)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    context.load_cert_chain(args.proxy, keyfile=args.proxy)
    conn = HTTPSConnection(url.netloc, context=context, timeout=60)

    if args.command == "jobs" and args.jobs == "list":
        conn.request("GET", "/arex/rest/1.0/jobs", headers={"Accept": "application/json"})
        resp = conn.getresponse()
        respstr = resp.read().decode()
        if resp.status != 200:
            print(f"ARC jobs list error: {resp.status} {respstr}")
        else:
            print(json.dumps(json.loads(respstr), indent=4))

    elif args.command == "jobs" and args.jobs == "info":
        tomanage = [{"id": arcid} for arcid in args.jobids]
        jsonData = {}
        if len(tomanage) == 1:
            jsonData["job"] = tomanage[0]
        else:
            jsonData["job"] = tomanage
        conn.request(
            "POST",
            "/arex/rest/1.0/jobs?action=info",
            body=json.dumps(jsonData).encode(),
            headers={"Accept": "application/json", "Content-type": "application/json"}
        )
        resp = conn.getresponse()
        respstr = resp.read().decode()
        if resp.status != 201:
            print(f"ARC jobs info error: {resp.status} {respstr}")
        else:
            print(json.dumps(json.loads(respstr), indent=4))

    elif args.command == "jobs" and args.jobs == "clean":
        tomanage = [{"id": arcid} for arcid in args.jobids]
        jsonData = {}
        if len(tomanage) == 1:
            jsonData["job"] = tomanage[0]
        else:
            jsonData["job"] = tomanage
        conn.request(
            "POST",
            "/arex/rest/1.0/jobs?action=clean",
            body=json.dumps(jsonData).encode(),
            headers={"Accept": "application/json", "Content-type": "application/json"}
        )
        resp = conn.getresponse()
        respstr = resp.read().decode()
        if resp.status != 201:
            print(f"ARC jobs info error: {resp.status} {respstr}")
        else:
            print(json.dumps(json.loads(respstr), indent=4))

    elif args.command == "jobs" and args.jobs == "submit":
        from pyarcrest.arc import ARCJob, ARCRest
        jobs = []
        for desc in args.jobdescs:
            with desc.open() as f:
                jobs.append(ARCJob(descstr=f.read()))
        arcrest = ARCRest(args.cluster, proxypath=args.proxy)
        queue = url.path.split("/")[-1]
        arcrest.submitJobs(queue, jobs)
        for job in jobs:
            print(job.id)

    elif args.command == "version":
        conn.request(
            "GET",
            "/arex/rest",
            headers={"Accept": "application/json"},
        )
        resp = conn.getresponse()
        print(json.dumps(json.loads(resp.read().decode()), indent=4))

    elif args.command == "info":
        conn.request(
            "GET",
            "/arex/rest/1.0/info",
            headers={"Accept": "application/json"},
        )
        resp = conn.getresponse()
        print(f"resp.status: {resp.status}")
        print(json.dumps(json.loads(resp.read().decode()), indent=4))
