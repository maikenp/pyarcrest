"""
Module for interaction with the ARC CE REST interface.

Automatic support for multiple versions of the API is implemented with optional
manual selection of the API version. This is done by defining a base class with
methods closely reflecting the operations specified in the ARC CE REST
interface specification: https://www.nordugrid.org/arc/arc6/tech/rest/rest.html
Additionally, the base class defines some higher level methods, e. g. a method
to upload job input files using multiple threads.

Some operations are implemented in class methods rather than regular instance
methods. Such operations are used to determine the API version or for threaded
contexts where internal state cannot be used (e. g. the http client).

The documentation of non instance methods should be carefully considered when
developing with them. None of the methods are static for the possible use case
of child classes overriding them and depending on other overriden class
behaviours. E. g. ARCRest._downloadFile() and ARCRest._uploadFile() do not
depend on any other class behaviour but their overrides could, like
ARCRest._downloadListing() does.

However, the basic use of the base class public API should be simple.
"""


import concurrent.futures
import datetime
import json
import os
import queue
import sys
import threading
from http.client import HTTPException
from urllib.parse import urlparse

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import arc
from pyarcrest.common import getNullLogger, HTTP_BUFFER_SIZE
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              MatchmakingError, MissingDiagnoseFile,
                              MissingOutputFile, NoValueInARCResult)
from pyarcrest.http import HTTPClient
from pyarcrest.x509 import parsePEM, signRequest


class ARCRest:

    @classmethod
    def getClient(cls, hostURL, apiBase="/arex", proxypath=None, logger=getNullLogger(), version=None, blocksize=None, timeout=None):
        httpClient = HTTPClient(hostURL, proxypath=proxypath, logger=logger, blocksize=blocksize, timeout=timeout)
        apiVersions = cls._getAPIVersions(httpClient, apiBase=apiBase)
        if not apiVersions:
            raise ARCError("No supported API versions")

        # /rest/1.0 compatibility
        if not isinstance(apiVersions["version"], list):
            apiVersions = [apiVersions["version"]]
        else:
            apiVersions = apiVersions["version"]

        if version is not None:
            if version not in apiVersions:
                raise ARCError(f"API version {version} not among supported ones {apiVersions}")
            apiVersion = version
        else:
            apiVersion = apiVersions[-1]

        if apiVersion == "1.0":
            return ARCRest_1_0(httpClient, apiBase=apiBase, logger=logger)
        elif apiVersion == "1.1":
            return ARCRest_1_1(httpClient, apiBase=apiBase, logger=logger)

    def __init__(self, httpClient, apiBase="/arex", logger=getNullLogger()):
        self.logger = logger
        self.apiBase = apiBase
        self.httpClient = httpClient

    def close(self):
        self.httpClient.close()

    def getAPIVersions(self):
        return self._getAPIVersions(self.httpClient, apiBase=self.apiBase)

    def getCEInfo(self):
        status, jsonData = self._requestJSON("GET", f"{self.apiPath}/info")
        if status != 200:
            raise ARCHTTPError(status, jsonData, f"Error getting ARC CE info - {status} {jsonData}")
        return self._loadJSON(status, jsonData)

    def getJobsList(self):
        status, jsonData = self._requestJSON("GET", f"{self.apiPath}/jobs")
        if status != 200:
            raise ARCHTTPError(status, jsonData, f"ARC jobs list error - {status} {jsonData}")
        jsonData = self._loadJSON(status, jsonData)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded number of upload workers
    def uploadJobFiles(self, jobs, workers=10, blocksize=None):
        # create transfer queues
        uploadQueue = queue.Queue()
        resultQueue = queue.Queue()

        # put uploads to queue, create cancel events for jobs
        jobsdict = {}
        for job in jobs:
            uploads = self._getInputUploads(job)
            if job.errors:
                self.logger.debug(f"Skipping job {job.id} due to input file errors")
                continue

            jobsdict[job.id] = job
            job.cancelEvent = threading.Event()

            for upload in uploads:
                uploadQueue.put(upload)
        if uploadQueue.empty():
            self.logger.debug("No local inputs to upload")
            return
        numWorkers = min(uploadQueue.qsize(), workers)

        # create HTTP clients for workers
        httpClients = []
        for i in range(numWorkers):
            httpClients.append(HTTPClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                isHTTPS=True,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
            ))

        self.logger.debug(f"Created {len(httpClients)} upload workers")

        # run upload threads on uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
            futures = []
            for httpClient in httpClients:
                futures.append(pool.submit(
                    self._uploadTransferWorker,
                    httpClient,
                    jobsdict,
                    uploadQueue,
                    resultQueue,
                    logger=self.logger,
                ))
            concurrent.futures.wait(futures)

        # close HTTP clients
        for httpClient in httpClients:
            httpClient.close()

        # put error messages to job dicts
        while not resultQueue.empty():
            result = resultQueue.get()
            resultQueue.task_done()
            job = jobsdict[result["jobid"]]
            job.errors.append(result["error"])

    def getJobsInfo(self, jobs):
        results = self._manageJobs(jobs, "info")
        for job, result in zip(jobs, results):
            if self._checkJobOperationSuccess(job, result, 200):
                if "info_document" not in result:
                    job.errors.append(NoValueInARCResult("No info document in successful info response"))
                else:
                    job.updateFromInfo(result["info_document"])

    def getJobsStatus(self, jobs):
        results = self._manageJobs(jobs, "status")
        for job, result in zip(jobs, results):
            if self._checkJobOperationSuccess(job, result, 200):
                if "state" not in result:
                    job.errors.append(NoValueInARCResult("No state in successful status response"))
                else:
                    job.state = result["state"]

    def killJobs(self, jobs):
        results = self._manageJobs(jobs, "kill")
        for job, result in zip(jobs, results):
            self._checkJobOperationSuccess(job, result, 202)

    def cleanJobs(self, jobs):
        results = self._manageJobs(jobs, "clean")
        for job, result in zip(jobs, results):
            self._checkJobOperationSuccess(job, result, 202)

    def restartJobs(self, jobs):
        results = self._manageJobs(jobs, "restart")
        for job, result in zip(jobs, results):
            self._checkJobOperationSuccess(job, result, 202)

    def getJobsDelegations(self, jobs, logger=None):
        results = self._manageJobs(jobs, "delegations")
        for job, result in zip(jobs, results):
            if self._checkJobOperationSuccess(job, result, 200):
                if "delegation_id" not in result:
                    job.errors.append(NoValueInARCResult("No delegation ID in successful response"))
                else:
                    job.delegid = result["delegation_id"]

    def downloadFile(self, url, path):
        self._downloadFile(self.httpClient, url, path)

    def uploadFile(self, url, path):
        self._uploadFile(self.httpClient, url, path)

    def downloadListing(self, url):
        self._downloadListing(self.httpClient, url)

    def createDelegation(self, lifetime=None):
        csr, delegationID = self._POSTNewDelegation()
        self._PUTDelegation(delegationID, csr, lifetime=lifetime)
        self.logger.debug(f"Successfully created delegation {delegationID}")
        return delegationID

    def renewDelegation(self, delegationID, lifetime=None):
        csr = self._POSTRenewDelegation(delegationID)
        self._PUTDelegation(delegationID, csr, lifetime=lifetime)
        self.logger.debug(f"Successfully renewed delegation {delegationID}")

    def deleteDelegation(self, delegationID):
        resp = self.httpClient.request(
            'POST',
            f'{self.apiPath}/delegations/{delegationID}?action=delete'
        )
        respstr = resp.read().decode()

        if resp.status != 202:
            raise ARCHTTPError(resp.status, respstr, f"Error deleting delegation {delegationID}: {resp.status} {respstr}")

        self.logger.debug(f"Successfully deleted delegation {delegationID}")

    # TODO: blocksize is only added in python 3.7!!!!!!!
    # TODO: hardcoded workers
    def downloadJobFiles(self, downloadDir, jobs, workers=10, blocksize=None):
        transferQueue = TransferQueue(workers)
        resultQueue = queue.Queue()

        jobsdict = {}
        for job in jobs:
            jobsdict[job.id] = job
            job.cancelEvent = threading.Event()

            # Add diagnose files to transfer queue and remove them from
            # downloadfiles string. Replace download files with a list of
            # remaining download patterns.
            self._processDiagnoseDownloads(job, transferQueue)

            # add job session directory as a listing transfer
            transferQueue.put({
                "jobid": job.id,
                "url": f"{self.apiPath}/jobs/{job.id}/session",
                "path": "",
                "type": "listing"
            })

        # open connections for thread workers
        httpClients = []
        for i in range(workers):
            httpClients.append(HTTPClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                isHTTPS=True,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
            ))

        self.logger.debug(f"Created {len(httpClients)} download workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for httpClient in httpClients:
                futures.append(pool.submit(
                    self._downloadTransferWorker,
                    httpClient,
                    transferQueue,
                    resultQueue,
                    downloadDir,
                    jobsdict,
                    self.apiPath,
                    logger=self.logger,
                    blocksize=blocksize,
                ))
            concurrent.futures.wait(futures)

        for httpClient in httpClients:
            httpClient.close()

        while not resultQueue.empty():
            result = resultQueue.get()
            jobsdict[result["jobid"]].errors.append(result["error"])
            resultQueue.task_done()

    def _processDiagnoseDownloads(self, job, transferQueue):
        DIAG_FILES = [
            "failed", "local", "errors", "description", "diag", "comment",
            "status", "acl", "xml", "input", "output", "input_status",
            "output_status", "statistics"
        ]

        if not job.downloadFiles:
            self.logger.debug(f"No files to download for job {job.id}")
            return []

        # add all diagnose files to transfer queue and create
        # a list of download patterns
        newDownloads = []
        diagFiles = set()  # to remove any possible duplications
        for download in job.downloadFiles:
            if download.startswith("diagnose="):
                # remove diagnose= part
                diagnose = download[len("diagnose="):]
                if not diagnose:
                    self.logger.debug(f"Skipping empty download entry: {download}")
                    continue  # error?

                # add all files if entire log folder is specified
                if diagnose.endswith("/"):
                    self.logger.debug(f"Will download all diagnose files to {diagnose}")
                    for diagFile in DIAG_FILES:
                        diagFiles.add(f"{diagnose}{diagFile}")

                else:
                    diagFile = diagnose.split("/")[-1]
                    if diagFile not in DIAG_FILES:
                        self.logger.debug(f"Skipping download {download} for because of unknown diagnose file {diagFile}")
                        continue  # error?
                    self.logger.debug(f"Will download diagnose file {diagFile} to {download}")
                    diagFiles.add(diagnose)
            else:
                self.logger.debug(f"Will download {download}")
                newDownloads.append(download)

        for diagFile in diagFiles:
            diagName = diagFile.split("/")[-1]
            transferQueue.put({
                "jobid": job.id,
                "url": f"{self.apiPath}/jobs/{job.id}/diagnose/{diagName}",
                "path": diagFile,
                "type": "diagnose"
            })

        job.downloadFiles = newDownloads

    def _POSTNewDelegation(self):
        resp = self.httpClient.request(
            "POST",
            f"{self.apiPath}/delegations?action=new",
        )
        respstr = resp.read().decode()

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot create delegation - {resp.status} {respstr}")

        return respstr, resp.getheader('Location').split('/')[-1]

    def _POSTRenewDelegation(self, delegationID):
        resp = self.httpClient.request(
            "POST",
            f"{self.apiPath}/delegations/{delegationID}?action=renew",
        )
        respstr = resp.read().decode()

        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot renew delegation {delegationID}: {resp.status} {respstr}")

        return respstr

    def _PUTDelegation(self, delegationID, csrStr, lifetime=None):
        try:
            with open(self.httpClient.proxypath) as f:
                proxyStr = f.read()

            proxyCert, _, issuerChains = parsePEM(proxyStr)
            chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
            csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
            cert = signRequest(csr, self.httpClient.proxypath, lifetime=lifetime).decode()
            pem = (cert + chain).encode()

            resp = self.httpClient.request(
                'PUT',
                f'{self.apiPath}/delegations/{delegationID}',
                data=pem,
                headers={'Content-Type': 'application/x-pem-file'}
            )
            respstr = resp.read().decode()

            if resp.status != 200:
                raise ARCHTTPError(resp.status, respstr, f"Cannot upload delegated cert: {resp.status} {respstr}")

        # cryptography exceptions are handled with the base exception so these
        # exceptions need to be handled explicitly to be passed through
        except (HTTPException, ConnectionError):
            raise

        except Exception as exc:
            try:
                self.deleteDelegation(delegationID)
            # ignore this error, delegations get deleted automatically anyway
            # and there is no simple way to encode both errors for now
            except (HTTPException, ConnectionError):
                pass
            raise ARCError(f"Error delegating proxy {self.httpClient.proxypath} for delegation {delegationID}: {exc}")

    def _manageJobs(self, jobs, action):
        ACTIONS = ("info", "status", "kill", "clean", "restart", "delegations")
        if not jobs:
            return []

        if action not in ACTIONS:
            raise ARCError(f"Invalid job management operation: {action}")

        # JSON data for request
        tomanage = [{"id": job.id} for job in jobs]
        # /rest/1.0 compatibility
        if len(tomanage) == 1:
            jsonData = {"job": tomanage[0]}
        else:
            jsonData = {"job": tomanage}

        # execute action and get JSON result
        status, jsonData = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs?action={action}",
            jsonData=jsonData,
        )
        if status != 201:
            raise ARCHTTPError(status, jsonData, f"ARC jobs \"{action}\" action error: {status} {jsonData}")
        jsonData = self._loadJSON(status, jsonData)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    def _getInputUploads(self, job):
        uploads = []
        for name, source in job.inputFiles.items():
            try:
                path = isLocalInputFile(name, source)
            except InputFileError as exc:
                job.errors.append(exc)
                self.logger.debug(f"Error parsing input {name} at {source} for job {job.id}: {exc}")
                continue
            if not path:
                self.logger.debug(f"Skipping non local input {name} at {source} for job {job.id}")
                continue

            if not os.path.isfile(path):
                msg = f"Local input {name} at {path} for job {job.id} is not a file"
                job.errors.append(InputFileError(msg))
                self.logger.debug(msg)
                continue

            uploads.append({
                "jobid": job.id,
                "url": f"{self.apiPath}/jobs/{job.id}/session/{name}",
                "path": path
            })
            self.logger.debug(f"Will upload local input {name} at {path} for job {job.id}")

        return uploads

    def _requestJSON(self, *args, **kwargs):
        return self._requestJSONStatic(self.httpClient, *args, **kwargs)

    def _checkJobOperationSuccess(self, job, result, success):
        code, reason = int(result["status-code"]), result["reason"]
        if code != success:
            job.errors.append(ARCHTTPError(code, reason, f"{code} {reason}"))
            return False
        else:
            return True

    @classmethod
    def _requestJSONStatic(cls, httpClient, *args, headers={}, **kwargs):
        headers["Accept"] = "application/json"
        resp = httpClient.request(*args, headers=headers, **kwargs)
        text = resp.read().decode()
        return resp.status, text

    @classmethod
    def _loadJSON(cls, status, jsonData):
        try:
            return json.loads(jsonData)
        except json.JSONDecodeError:
            raise ARCHTTPError(status, jsonData, f"Invalid JSON data in successful response - {status} {jsonData}")

    @classmethod
    def _getAPIVersions(cls, httpClient, apiBase="/arex"):
        status, jsonData = cls._requestJSONStatic(httpClient, "GET", f"{apiBase}/rest")
        if status != 200:
            raise ARCHTTPError(status, jsonData, f"Error getting ARC API versions - {status} {jsonData}")
        return cls._loadJSON(status, jsonData)

    @classmethod
    def _downloadFile(cls, httpClient, url, path, blocksize=None):
        if blocksize is None:
            blocksize = HTTP_BUFFER_SIZE

        resp = httpClient.request("GET", url)

        if resp.status != 200:
            text = resp.read().decode()
            raise ARCHTTPError(resp.status, text, f"Error downloading URL {url} to {path}: {resp.status} {text}")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            data = resp.read(blocksize)
            while data:
                f.write(data)
                data = resp.read(blocksize)

    @classmethod
    def _uploadFile(cls, httpClient, url, path):
        with open(path, "rb") as f:
            resp = httpClient.request("PUT", url, data=f)
            text = resp.read().decode()
            if resp.status != 200:
                raise ARCHTTPError(resp.status, text, f"Upload {path} to {url} failed: {resp.status} {text}")

    @classmethod
    def _downloadListing(cls, httpClient, url):
        status, jsonData = cls._requestJSONStatic(httpClient, "GET", url)
        if status != 200:
            raise ARCHTTPError(status, jsonData, f"Error downloading listing {url}: {status} {jsonData}")
        return cls._loadJSON(status, jsonData)

    @classmethod
    def _createTransfersFromListing(cls, downloadFiles, endpoint, listing, path, jobid):
        transfers = []
        if "file" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["file"], list):
                listing["file"] = [listing["file"]]
            for f in listing["file"]:
                if path:
                    newpath = f"{path}/{f}"
                else:  # if session root, slash needs to be skipped
                    newpath = f
                if not cls._filterOutFile(downloadFiles, newpath):
                    transfers.append({
                        "jobid": jobid,
                        "type": "file",
                        "path": newpath,
                        "url": f"{endpoint}/jobs/{jobid}/session/{newpath}"
                    })
        if "dir" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["dir"], list):
                listing["dir"] = [listing["dir"]]
            for d in listing["dir"]:
                if path:
                    newpath = f"{path}/{d}"
                else:  # if session root, slash needs to be skipped
                    newpath = d
                if not cls._filterOutListing(downloadFiles, newpath):
                    transfers.append({
                        "jobid": jobid,
                        "type": "listing",
                        "path": newpath,
                        "url": f"{endpoint}/jobs/{jobid}/session/{newpath}"
                    })
        return transfers

    @classmethod
    def _uploadTransferWorker(cls, httpClient, jobsdict, uploadQueue, resultQueue, logger=getNullLogger()):
        while True:
            try:
                upload = uploadQueue.get(block=False)
            except queue.Empty:
                break
            uploadQueue.task_done()

            job = jobsdict[upload["jobid"]]
            if job.cancelEvent.is_set():
                logger.debug(f"Skipping upload for cancelled job {upload['jobid']}")
                continue

            try:
                cls._uploadFile(httpClient, upload["url"], upload["path"])
            except Exception as exc:
                job.cancelEvent.set()
                resultQueue.put({
                    "jobid": upload["jobid"],
                    "error": exc,
                })
                if isinstance(exc, ARCHTTPError):
                    logger.debug(str(exc))
                else:
                    logger.debug(f"Upload {upload['path']} to {upload['url']} for job {upload['jobid']} failed: {exc}")

    @classmethod
    def _downloadTransferWorker(cls, httpClient, transferQueue, resultQueue, downloadDir, jobsdict, endpoint, logger=getNullLogger(), blocksize=None):
        while True:
            try:
                transfer = transferQueue.get()
            except TransferQueueEmpty():
                break

            job = jobsdict[transfer["jobid"]]
            if job.cancelEvent.is_set():
                logger.debug(f"Skipping download for cancelled job {transfer['jobid']}")
                continue

            try:
                if transfer["type"] in ("file", "diagnose"):
                    # download file
                    path = f"{downloadDir}/{transfer['jobid']}/{transfer['path']}"
                    try:
                        cls._downloadFile(httpClient, transfer["url"], path, blocksize=blocksize)
                    except Exception as exc:
                        error = exc
                        if isinstance(exc, ARCHTTPError):
                            if exc.status == 404:
                                if transfer["type"] == "diagnose":
                                    error = MissingDiagnoseFile(transfer["url"])
                                else:
                                    error = MissingOutputFile(transfer["url"])
                        else:
                            job.cancelEvent.set()

                        resultQueue.put({
                            "jobid": transfer["jobid"],
                            "error": error
                        })
                        logger.debug(f"Download {transfer['url']} to {path} for job {transfer['jobid']} failed: {error}")
                    else:
                        logger.debug(f"Download {transfer['url']} to {path} for job {transfer['jobid']} successful")

                elif transfer["type"] == "listing":
                    # download listing
                    try:
                        listing = cls._downloadListing(httpClient, transfer["url"])
                    except ARCHTTPError as exc:
                        if exc.text == "":
                            listing = {}
                        else:
                            raise
                    except Exception as exc:
                        if not isinstance(exc, ARCHTTPError):
                            job.cancelEvent.set()
                        resultQueue.put({
                            "jobid": transfer["jobid"],
                            "error": exc
                        })
                        logger.debug(f"Download listing {transfer['url']} for job {transfer['jobid']} failed: {exc}")
                    else:
                        # create new transfer jobs
                        transfers = cls._createTransfersFromListing(
                            job.downloadFiles, endpoint, listing, transfer["path"], transfer["jobid"]
                        )
                        for transfer in transfers:
                            transferQueue.put(transfer)
                        logger.debug(f"Download listing {transfer['url']} for job {transfer['jobid']} successful: {listing}")

            # every possible exception needs to be handled, otherwise the
            # threads will lock up
            except:
                import traceback
                excstr = traceback.format_exc()
                job.cancelEvent.set()
                resultQueue.put({
                    "jobid": transfer["jobid"],
                    "error": excstr
                })
                logger.debug(f"Download URL {transfer['url']} and path {transfer['path']} for job {transfer['jobid']} failed: {excstr}")

    @classmethod
    def _filterOutFile(cls, downloadFiles, filePath):
        if not downloadFiles:
            return False
        for pattern in downloadFiles:
            # direct match
            if pattern == filePath:
                return False
            # recursive folder match
            elif pattern.endswith("/") and filePath.startswith(pattern):
                return False
            # entire session directory, not matched by above if
            elif pattern == "/":
                return False
        return True


    @classmethod
    def _filterOutListing(cls, downloadFiles, listingPath):
        if not downloadFiles:
            return False
        for pattern in downloadFiles:
            # part of pattern
            if pattern.startswith(listingPath):
                return False
            # recursive folder match
            elif pattern.endswith("/") and listingPath.startswith(pattern):
                return False
        return True


class ARCRest_1_0(ARCRest):

    def __init__(self, httpClient, apiBase="/arex", logger=getNullLogger()):
        super().__init__(httpClient, apiBase=apiBase, logger=logger)
        self.apiPath = f"{apiBase}/rest/1.0"

    def getAPIPath(self):
        return self.apiPath

    def _findQueue(self, queue, ceInfo):
        compShares = ceInfo.get("Domains", {}) \
                           .get("AdminDomain", {}) \
                           .get("Services", {}) \
                           .get("ComputingService", {}) \
                           .get("ComputingShare", [])
        if not compShares:
            raise ARCError("No queues found on cluster")

        # /rest/1.0 compatibility
        if isinstance(compShares, dict):
            compShares = [compShares]

        for compShare in compShares:
            if compShare.get("Name", None) == queue:
                # Queues are defined as ComputingShares. There are some shares
                # that are mapped to another share. Such a share is never a
                # queue externally. So if the name of the such share is used as
                # a queue, the result has to be empty.
                if "MappingPolicy" in compShare:
                    return None
                else:
                    return compShare
        return None

    def _findRuntimes(self, ceInfo):
        appenvs = ceInfo.get("Domains", {}) \
                        .get("AdminDomain", {}) \
                        .get("Services", {}) \
                        .get("ComputingService", {}) \
                        .get("ComputingManager", {}) \
                        .get("ApplicationEnvironments", {}) \
                        .get("ApplicationEnvironment", [])

        # /rest/1.0 compatibility
        if isinstance(appenvs, dict):
            appenvs = [appenvs]

        runtimes = []
        for env in appenvs:
            if "AppName" in env:
                envname = env["AppName"]
                if "AppVersion" in env:
                    envname += f"-{env['AppVersion']}"
                runtimes.append(envname)
        return runtimes

    def submitJobs(self, queue, jobs, uploadData=True):
        ceInfo = self.getCEInfo()
        queueInfo = self._findQueue(queue, ceInfo)
        if queueInfo is None:
            raise MatchmakingError(f"Requested queue {queue} does not exist")
        runtimes = self._findRuntimes(ceInfo)

        # get delegation for proxy
        delegationID = self.createDelegation()

        jobdescs = arc.JobDescriptionList()
        tosubmit = []  # sublist of jobs that will be submitted
        bulkdesc = ""
        for job in jobs:
            job.delegid = delegationID

            # parse job description
            if not arc.JobDescription_Parse(job.descstr, jobdescs):
                job.errors.append(DescriptionParseError("Failed to parse description"))
                self.logger.debug(f"Failed to parse description {job.descstr}")
                continue
            desc = jobdescs[-1]

            # matchmaking
            #
            # TODO: only direct version comparison is done for runtime
            # envrionment; some other parameters are not matched:
            # - memory (xrsl): which value is it from info document?
            #   MaxVirtualMemory?
            # - disk: no relevant value found in info document
            cpuTime = desc.Resources.TotalCPUTime.range.max
            wallTime = desc.Resources.TotalWallTime.range.max
            envs = [str(env) for env in desc.Resources.RunTimeEnvironment.getSoftwareList()]
            maxWallTime = int(queueInfo.get("MaxWallTime", sys.maxsize))
            error = None
            if wallTime > maxWallTime:
                error = MatchmakingError(f"Requested wall time {wallTime} higher than available {maxWallTime}")
            else:
                for env in envs:
                    if env not in runtimes:
                        error = MatchmakingError(f"Requested environment {env} not available")
                        break
            if error is not None:
                job.errors.append(error)
                self.logger.debug(str(error))
                continue

            # add queue and delegation, modify description as necessary for
            # ARC client
            desc.Resources.QueueName = queue
            desc.DataStaging.DelegationID = delegationID
            processJobDescription(desc)

            # get input files from description
            job.getArclibInputFiles(desc)

            # read name from description
            job.name = desc.Identification.JobName
            self.logger.debug(f"Job name from description: {job.name}")

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = desc.UnParse("emies:adl")
            if not unparseResult[0]:
                job.errors.append(DescriptionUnparseError(f"Could not unparse modified description of job {job.name}"))
                self.logger.debug(f"Could not unparse modified description of job {job.name}")
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append(job)

        if not tosubmit:
            return

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

        # submit jobs to ARC
        status, jsonData = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs?action=new",
            data=bulkdesc,
            headers={"Content-Type": "application/xml"},
        )
        if status != 201:
            raise ARCHTTPError(status, jsonData, f"Error submitting jobs - {status} {jsonData}")
        jsonData = self._loadJSON(status, jsonData)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            results = [jsonData["job"]]
        else:
            results = jsonData["job"]

        # process errors, prepare and upload files for a sublist of jobs
        toupload = []
        for job, result in zip(tosubmit, results):
            if self._checkJobOperationSuccess(job, result, 201):
                job.id = result["id"]
                job.state = result["state"]
                toupload.append(job)
        if uploadData:
            self.uploadJobFiles(toupload)


class ARCRest_1_1(ARCRest):

    def __init__(self, httpClient, apiBase="/arex", logger=getNullLogger()):
        super().__init__(httpClient, apiBase=apiBase, logger=logger)
        self.apiPath = f"{apiBase}/rest/1.1"

    def getAPIPath(self):
        return self.apiPath


class ARCJob:

    def __init__(self, id=None, descstr=None):
        self.id = id
        self.descstr = descstr
        self.name = None
        self.delegid = None
        self.state = None
        self.tstate = None
        self.cancelEvent = None
        self.errors = []
        self.downloadFiles = []
        self.inputFiles = {}

        self.ExecutionNode = None
        self.UsedTotalWallTime = None
        self.UsedTotalCPUTime = None
        self.RequestedTotalWallTime = None
        self.RequestedTotalCPUTime = None
        self.RequestedSlots = None
        self.ExitCode = None
        self.Type = None
        self.LocalIDFromManager = None
        self.WaitingPosition = None
        self.Owner = None
        self.LocalOwner = None
        self.StdIn = None
        self.StdOut = None
        self.StdErr = None
        self.LogDir = None
        self.Queue = None
        self.UsedMainMemory = None
        self.SubmissionTime = None
        self.EndTime = None
        self.WorkingAreaEraseTime = None
        self.ProxyExpirationTime = None
        self.RestartState = []
        self.Error = []

    def updateFromInfo(self, infoDocument):
        infoDict = infoDocument.get("ComputingActivity", {})
        if not infoDict:
            return

        if "Name" in infoDict:
            self.name = infoDict["Name"]

        # get state from a list of activity states in different systems
        for state in infoDict.get("State", []):
            if state.startswith("arcrest:"):
                self.state = state[len("arcrest:"):]

        if "Error" in infoDict:
            # /rest/1.0 compatibility
            if isinstance(infoDict["Error"], list):
                self.Error = infoDict["Error"]
            else:
                self.Error = [infoDict["Error"]]

        if "ExecutionNode" in infoDict:
            # /rest/1.0 compatibility
            if isinstance(infoDict["ExecutionNode"], list):
                self.ExecutionNode = infoDict["ExecutionNode"]
            else:
                self.ExecutionNode = [infoDict["ExecutionNode"]]
            # throw out all non ASCII characters from nodes
            for i in range(len(self.ExecutionNode)):
                self.ExecutionNode[i] = ''.join([i for i in self.ExecutionNode[i] if ord(i) < 128])

        if "UsedTotalWallTime" in infoDict:
            self.UsedTotalWallTime = int(infoDict["UsedTotalWallTime"])

        if "UsedTotalCPUTime" in infoDict:
            self.UsedTotalCPUTime = int(infoDict["UsedTotalCPUTime"])

        if "RequestedTotalWallTime" in infoDict:
            self.RequestedTotalWallTime = int(infoDict["RequestedTotalWallTime"])

        if "RequestedTotalCPUTime" in infoDict:
            self.RequestedTotalCPUTime = int(infoDict["RequestedTotalCPUTime"])

        if "RequestedSlots" in infoDict:
            self.RequestedSlots = int(infoDict["RequestedSlots"])

        if "ExitCode" in infoDict:
            self.ExitCode = int(infoDict["ExitCode"])

        if "Type" in infoDict:
            self.Type = infoDict["Type"]

        if "LocalIDFromManager" in infoDict:
            self.LocalIDFromManager = infoDict["LocalIDFromManager"]

        if "WaitingPosition" in infoDict:
            self.WaitingPosition = int(infoDict["WaitingPosition"])

        if "Owner" in infoDict:
            self.Owner = infoDict["Owner"]

        if "LocalOwner" in infoDict:
            self.LocalOwner = infoDict["LocalOwner"]

        if "StdIn" in infoDict:
            self.StdIn = infoDict["StdIn"]

        if "StdOut" in infoDict:
            self.StdOut = infoDict["StdOut"]

        if "StdErr" in infoDict:
            self.StdErr = infoDict["StdErr"]

        if "LogDir" in infoDict:
            self.LogDir = infoDict["LogDir"]

        if "Queue" in infoDict:
            self.Queue = infoDict["Queue"]

        if "UsedMainMemory" in infoDict:
            self.UsedMainMemory = int(infoDict["UsedMainMemory"])

        if "SubmissionTime" in infoDict:
            self.SubmissionTime = datetime.datetime.strptime(
                infoDict["SubmissionTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "EndTime" in infoDict:
            self.EndTime = datetime.datetime.strptime(
                infoDict["EndTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "WorkingAreaEraseTime" in infoDict:
            self.WorkingAreaEraseTime = datetime.datetime.strptime(
                infoDict["WorkingAreaEraseTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "ProxyExpirationTime" in infoDict:
            self.ProxyExpirationTime = datetime.datetime.strptime(
                infoDict["ProxyExpirationTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "RestartState" in infoDict:
            self.RestartState = infoDict["RestartState"]

    def getArclibInputFiles(self, desc):
        self.inputFiles = {}
        for infile in desc.DataStaging.InputFiles:
            source = None
            if len(infile.Sources) > 0:
                source = infile.Sources[0].fullstr()
            self.inputFiles[infile.Name] = source


class TransferQueue:

    def __init__(self, numWorkers):
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(numWorkers)

    def put(self, val):
        with self.lock:
            self.queue.put(val)
            self.barrier.reset()

    def get(self):
        while True:
            with self.lock:
                if not self.queue.empty():
                    val = self.queue.get()
                    self.queue.task_done()
                    return val

            try:
                self.barrier.wait()
            except threading.BrokenBarrierError:
                continue
            else:
                raise TransferQueueEmpty()


def isLocalInputFile(name, path):
    """
    Return path if local or empty string if remote URL.

    Raises:
        - InputFileError
    """
    if not path:
        return name

    try:
        url = urlparse(path)
    except ValueError as exc:
        raise InputFileError(f"Error parsing source {path} of file {name}: {exc}")
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""

    return url.path


def processJobDescription(jobdesc):
    exepath = jobdesc.Application.Executable.Path
    if exepath and exepath.startswith("/"):  # absolute paths are on compute nodes
        exepath = ""
    inpath = jobdesc.Application.Input
    outpath = jobdesc.Application.Output
    errpath = jobdesc.Application.Error
    logpath = jobdesc.Application.LogDir

    exePresent = False
    stdinPresent = False
    for infile in jobdesc.DataStaging.InputFiles:
        if exepath == infile.Name:
            exePresent = True
        elif inpath == infile.Name:
            stdinPresent = True

    stdoutPresent = False
    stderrPresent = False
    logPresent = False
    for outfile in jobdesc.DataStaging.OutputFiles:
        if outpath == outfile.Name:
            stdoutPresent = True
        elif errpath == outfile.Name:
            stderrPresent = True
        elif logpath == outfile.Name or logpath == outfile.Name[:-1]:
            logPresent = True

    if exepath and not exePresent:
        infile = arc.InputFileType()
        infile.Name = exepath
        jobdesc.DataStaging.InputFiles.append(infile)

    if inpath and not stdinPresent:
        infile = arc.InputFileType()
        infile.Name = inpath
        jobdesc.DataStaging.InputFiles.append(infile)

    if outpath and not stdoutPresent:
        outfile = arc.OutputFileType()
        outfile.Name = outpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if errpath and not stderrPresent:
        outfile = arc.OutputFileType()
        outfile.Name = errpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if logpath and not logPresent:
        outfile = arc.OutputFileType()
        if not logpath.endswith('/'):
            outfile.Name = f'{logpath}/'
        else:
            outfile.Name = logpath
        jobdesc.DataStaging.OutputFiles.append(outfile)


class TransferQueueEmpty(Exception):
    pass
