"""
Module for interaction with the ARC CE REST interface.

Automatic support for multiple versions of the API is implemented with optional
manual selection of the API version. This is done by defining a base class with
methods closely reflecting the operations specified in the ARC CE REST
interface specification: https://www.nordugrid.org/arc/arc6/tech/rest/rest.html
Additionally, the base class defines some higher level methods, e. g. a method
to upload job input files using multiple threads.

Some operations involved in determining the API version are implemented in class
methods instead of instance methods as instance methods are considered to be
tied to the API version. Determination of API version should therefore be a
static operation.
"""


import concurrent.futures
import datetime
import json
import os
import queue
import threading
from urllib.parse import urlparse

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pyarcrest.common import HTTP_BUFFER_SIZE, getNullLogger
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              InputUploadError, MatchmakingError,
                              MissingDiagnoseFile, MissingOutputFile,
                              NoValueInARCResult)
from pyarcrest.http import HTTPClient
from pyarcrest.x509 import parsePEM, signRequest

# TODO: blocksize can only be used with Python >= 3.7


class ARCRest:

    def __init__(self, httpClient, apiBase="/arex", logger=getNullLogger()):
        """
        Initialize the base object.

        Note that this class should not be instantiated directly because
        additional implementations of attributes and methods are required from
        derived classes.
        """
        self.logger = logger
        self.apiBase = apiBase
        self.httpClient = httpClient

    def close(self):
        self.httpClient.close()

    ### Direct operations on ARC CE ###

    def getAPIVersions(self):
        return self.getAPIVersionsStatic(self.httpClient, self.apiBase)

    def getCEInfo(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/info")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting ARC CE info: {status} {text}")
        return json.loads(text)

    def getJobsList(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/jobs")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting jobs list: {status} {text}")

        # /rest/1.0 compatibility
        try:
            jsonData = json.loads(text)["job"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                jsonData = []
            else:
                raise
        if isinstance(jsonData, dict):
            jsonData = [jsonData]

        return [job["id"] for job in jsonData]

    def createJobs(self, description, delegationID=None, queue=None, isADL=True):
        raise Exception("Not implemented in the base class")

    def getJobsInfo(self, jobs):
        responses = self._manageJobs(jobs, "info")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason, f"Error getting info for job {job}: {code} {reason}"))
            elif "info_document" not in response:
                results.append(NoValueInARCResult(f"No info document in successful info response for job {job}"))
            else:
                results.append(self._parseJobInfo(response["info_document"]))
        return results

    def getJobsStatus(self, jobs):
        responses = self._manageJobs(jobs, "status")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason, f"Error getting status for job {job}: {code} {reason}"))
            elif "state" not in response:
                results.append(NoValueInARCResult("No state in successful status response"))
            else:
                results.append(response["state"])
        return results

    def killJobs(self, jobs):
        responses = self._manageJobs(jobs, "kill")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason, f"Error killing job {job}: {code} {reason}"))
            else:
                results.append(True)
        return results

    def cleanJobs(self, jobs):
        responses = self._manageJobs(jobs, "clean")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason, f"Error cleaning job {job}: {code} {reason}"))
            else:
                results.append(True)
        return results

    def restartJobs(self, jobs):
        responses = self._manageJobs(jobs, "restart")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason, f"Error restarting job {job}: {code} {reason}"))
            else:
                results.append(True)
        return results

    def getJobsDelegations(self, jobs):
        responses = self._manageJobs(jobs, "delegations")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason, f"Error getting delegations for job {job}: {code} {reason}"))
            elif "delegation_id" not in response:
                results.append(NoValueInARCResult("No delegation ID in successful response"))
            else:
                # /rest/1.0 compatibility
                if isinstance(response["delegation_id"], list):
                    results.append(response["delegation_id"])
                else:
                    results.append([response["delegation_id"]])
        return results

    def downloadFile(self, url, path, blocksize=None):
        if blocksize is None:
            blocksize = HTTP_BUFFER_SIZE

        resp = self.httpClient.request("GET", url)

        if resp.status != 200:
            text = resp.read().decode()
            raise ARCHTTPError(resp.status, text, f"Error downloading URL {url} to {path}: {resp.status} {text}")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            data = resp.read(blocksize)
            while data:
                f.write(data)
                data = resp.read(blocksize)

    def uploadFile(self, url, path):
        with open(path, "rb") as f:
            resp = self.httpClient.request("PUT", url, data=f)
            text = resp.read().decode()
            if resp.status != 200:
                raise ARCHTTPError(resp.status, text, f"Error uploading {path} to {url}: {resp.status} {text}")

    def downloadListing(self, url):
        status, text = self._requestJSON("GET", url)
        if status != 200:
            raise ARCHTTPError(status, text, f"Error downloading listing {url}: {status} {text}")

        # /rest/1.0 compatibility
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return {}
            else:
                raise

    def getDelegationsList(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/delegations")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting delegations list: {status} {text}")

        # /rest/1.0 compatibility
        try:
            return json.loads(text)["delegation"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return []
            else:
                raise

    # Returns a tuple of CSR and delegation ID
    def requestNewDelegation(self):
        url = f"{self.apiPath}/delegations?action=new"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot get delegation CSR: {resp.status} {respstr}")
        return respstr, resp.getheader("Location").split("/")[-1]

    def uploadDelegation(self, delegationID, signedCert):
        url = f"{self.apiPath}/delegations/{delegationID}"
        headers = {"Content-Type": "application/x-pem-file"}
        resp = self.httpClient.request("PUT", url, data=signedCert, headers=headers)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot upload delegated cert for delegation {delegationID}: {resp.status} {respstr}")

    def getDelegationCert(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=get"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot get cert for delegation {delegationID}: {resp.status} {respstr}")
        return respstr

    # returns CSR
    def requestDelegationRenewal(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=renew"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot renew delegation {delegationID}: {resp.status} {respstr}")
        return respstr

    def deleteDelegation(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=delete"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot delete delegation {delegationID}: {resp.status} {respstr}")

    ### Higher level job operations ###

    # TODO: HARDCODED
    def uploadJobFiles(self, jobids, jobInputs, workers=10, blocksize=None, timeout=None):
        resultDict = {jobid: [] for jobid in jobids}

        # create upload queue
        uploadQueue = queue.Queue()
        for jobid, inputFiles in zip(jobids, jobInputs):
            errors = self._addInputTransfers(uploadQueue, jobid, inputFiles)
            if errors:
                self.logger.debug(f"Skipping job {jobid} due to input file errors")
                resultDict[jobid].extend(errors)

        if uploadQueue.empty():
            self.logger.debug("No local inputs to upload")
            return [resultDict[jobid] for jobid in jobids]

        errorQueue = queue.Queue()

        # create REST clients for workers
        numWorkers = min(uploadQueue.qsize(), workers)
        restClients = []
        for i in range(numWorkers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
                timeout=timeout,
                apiBase=self.apiBase,
                version=self.version,
            ))
        self.logger.debug(f"Created {len(restClients)} upload workers")

        # run upload threads on upload queue
        with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._uploadTransferWorker,
                    restClient,
                    uploadQueue,
                    errorQueue,
                    logger=self.logger,
                ))
            concurrent.futures.wait(futures)

        # close HTTP clients
        for restClient in restClients:
            restClient.close()

        # get transfer errors
        while not errorQueue.empty():
            error = errorQueue.get()
            resultDict[error["jobid"]].append(error["error"])
            errorQueue.task_done()

        return [resultDict[jobid] for jobid in jobids]

    # TODO: HARDCODED
    def downloadJobFiles(self, downloadDir, jobids, jobsDownloads, workers=10, blocksize=None, timeout=None):
        transferQueue = TransferQueue(workers)

        downloadDict = {}
        for jobid, downloadFiles in zip(jobids, jobsDownloads):
            downloadDict[jobid] = downloadFiles
            cancelEvent = threading.Event()

            # add diagnose files to transfer queue
            self._addDiagnoseTransfers(transferQueue, jobid, downloadFiles, cancelEvent)

            # add job session directory as a listing transfer
            url = f"{self.apiPath}/jobs/{jobid}/session"
            transferQueue.put(FileTransfer(jobid, url, "", cancelEvent=cancelEvent, type="listing"))

        errorQueue = queue.Queue()

        # create REST clients for workers
        restClients = []
        for i in range(workers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
                timeout=timeout,
                apiBase=self.apiBase,
                version=self.version,
            ))

        self.logger.debug(f"Created {len(restClients)} download workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._downloadTransferWorker,
                    restClient,
                    transferQueue,
                    errorQueue,
                    downloadDir,
                    downloadDict,
                    logger=self.logger,
                ))
            concurrent.futures.wait(futures)

        for restClient in restClients:
            restClient.close()

        # get transfer errors
        resultDict = {jobid: [] for jobid in jobids}
        while not errorQueue.empty():
            error = errorQueue.get()
            resultDict[error["jobid"]].append(error["error"])
            errorQueue.task_done()

        return [resultDict[jobid] for jobid in jobids]

    def createDelegation(self, lifetime=None):
        csr, delegationID = self.requestNewDelegation()
        pem = self._signCSR(csr, lifetime=lifetime)
        self.uploadDelegation(delegationID, pem)
        return delegationID

    def renewDelegation(self, delegationID, lifetime=None):
        csr = self.requestDelegationRenewal(delegationID)
        pem = self._signCSR(csr, lifetime=lifetime)
        try:
            self.uploadDelegation(delegationID, pem)
        except Exception:
            self.deleteDelegation(delegationID)
            raise

    def submitJobs(self, delegationID, descs, queue, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        raise Exception("Not implemented in the base class")

    # TODO: should queue be mandatory parameter for this client?
    def matchJob(self, ceInfo, queue, runtimes=[], walltime=None):
        errors = []

        if queue:
            try:
                self._matchQueue(ceInfo, queue)
            except MatchmakingError as exc:
                errors.append(exc)

        if runtimes:
            for runtime in runtimes:
                try:
                    self._matchRuntime(ceInfo, runtime)
                except MatchmakingError as exc:
                    errors.append(exc)

        if queue and walltime:
            try:
                self.matchWalltime(ceInfo, queue, walltime)
            except MatchmakingError as exc:
                errors.append(exc)

        return errors

    ### Private support methods

    # returns nothing if match successful, raises exception otherwise
    def _matchQueue(self, ceInfo, queue):
        if not self._findQueue(ceInfo, queue):
            raise MatchmakingError(f"Queue {queue} not found to match walltime")

    # returns nothing if match successful, raises exception otherwise
    def _matchRuntime(self, ceInfo, runtime):
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

        for env in appenvs:
            if "AppName" in env:
                envname = env["AppName"]
                if "AppVersion" in env:
                    envname += f"-{env['AppVersion']}"
                if runtime == envname:
                    return

        raise MatchmakingError(f"Runtime {runtime} not found")

    # returns nothing if match successful, raises exception otherwise
    def _matchWalltime(self, ceInfo, queue, walltime):
        queueInfo = self._findQueue(ceInfo, queue)
        if queueInfo is None:
            raise MatchmakingError(f"Queue {queue} not found to match walltime")

        if "MaxWallTime" in queueInfo:
            maxWallTime = int(queueInfo["MaxWallTime"])
            if walltime > maxWallTime:
                raise MatchmakingError(f"Walltime {walltime} higher than max walltime {maxWallTime} for queue {queue}")

    def _signCSR(self, csrStr, lifetime=None):
        with open(self.httpClient.proxypath) as f:
            proxyStr = f.read()
        proxyCert, _, issuerChains = parsePEM(proxyStr)
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
        cert = signRequest(csr, self.httpClient.proxypath, lifetime=lifetime).decode()
        pem = (cert + chain).encode()
        return pem

    def _addInputTransfers(self, uploadQueue, jobid, inputFiles):
        cancelEvent = threading.Event()
        errors = []
        for name, source in inputFiles.items():
            try:
                path = isLocalInputFile(name, source)
            except ValueError as exc:
                error = InputFileError(f"Error parsing source {source} of input {name}: {exc}")
                errors.append(error)
                self.logger.debug(error)
                continue
            if not path:
                self.logger.debug(f"Skipping non local input {name} at {source} for job {jobid}")
                continue
            if not os.path.isfile(path):
                error = InputFileError(f"Source {source} of input {name} is not a file")
                errors.append(error)
                self.logger.debug(error)
                continue
            url = f"{self.apiPath}/jobs/{jobid}/session/{name}"
            uploadQueue.put(FileTransfer(jobid, url, path, cancelEvent=cancelEvent))
            self.logger.debug(f"Will upload local input {name} at {path} for job {jobid}")
        return errors

    def _addDiagnoseTransfers(self, transferQueue, jobid, downloadFiles, cancelEvent):
        DIAG_FILES = [
            "failed", "local", "errors", "description", "diag", "comment",
            "status", "acl", "xml", "input", "output", "input_status",
            "output_status", "statistics"
        ]
        # add all diagnose files to transfer queue
        diagFiles = set()  # set instead of list to remove possible duplications
        for download in downloadFiles:
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
                        self.logger.debug(f"Skipping download {download} because of unknown diagnose file {diagFile}")
                        continue  # error?
                    self.logger.debug(f"Will download diagnose file {diagFile} to {download}")
                    diagFiles.add(diagnose)

        for diagFile in diagFiles:
            diagName = diagFile.split("/")[-1]
            url = f"{self.apiPath}/jobs/{jobid}/diagnose/{diagName}"
            transferQueue.put(FileTransfer(jobid, url, diagFile, cancelEvent=cancelEvent, type="diagnose"))

    def _requestJSON(self, *args, **kwargs):
        return self._requestJSONStatic(self.httpClient, *args, **kwargs)

    def _manageJobs(self, jobs, action):
        if not jobs:
            return []

        # JSON data for request
        tomanage = [{"id": job} for job in jobs]

        # /rest/1.0 compatibility
        if len(tomanage) == 1:
            jsonData = {"job": tomanage[0]}
        else:
            jsonData = {"job": tomanage}

        # execute action and get JSON result
        url = f"{self.apiPath}/jobs?action={action}"
        status, text = self._requestJSON("POST", url, jsonData=jsonData)
        if status != 201:
            raise ARCHTTPError(status, text, f"ARC jobs \"{action}\" action error: {status} {text}")
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    def _addTransfersFromListing(self, transferQueue, jobid, downloadFiles, listing, path, cancelEvent):
        if "file" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["file"], list):
                listing["file"] = [listing["file"]]

            for f in listing["file"]:
                if path:
                    newpath = f"{path}/{f}"
                else:  # if session root, slash needs to be skipped
                    newpath = f
                if not self._filterOutFile(downloadFiles, newpath):
                    url = f"{self.apiPath}/jobs/{jobid}/session/{newpath}"
                    transferQueue.put(FileTransfer(jobid, url, newpath, cancelEvent, type="file"))
        if "dir" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["dir"], list):
                listing["dir"] = [listing["dir"]]

            for d in listing["dir"]:
                if path:
                    newpath = f"{path}/{d}"
                else:  # if session root, slash needs to be skipped
                    newpath = d
                if not self._filterOutListing(downloadFiles, newpath):
                    url = f"{self.apiPath}/jobs/{jobid}/session/{newpath}"
                    transferQueue.put(FileTransfer(jobid, url, newpath, cancelEvent, type="listing"))

    def _filterOutFile(self, downloadFiles, filePath):
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

    def _filterOutListing(self, downloadFiles, listingPath):
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

    def _findQueue(self, ceInfo, queue):
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

    # TODO: think about what to log and how
    def _submitJobs(self, delegationID, descs, queue, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None, v1_0=False):
        import arc
        ceInfo = self.getCEInfo()

        # A list of tuples of index and input file dict for every job
        # description to be submitted. The index is the description's
        # position in the given parameter of job descriptions and is
        # required to create properly aligned results.
        tosubmit = []

        # A dict of a key that is index in given descs list and a value that
        # is either a list of exceptions for failed submission or a tuple of
        # jobid and state for successful submission.
        resultDict = {}

        jobdescs = arc.JobDescriptionList()
        bulkdesc = ""
        for i in range(len(descs)):
            # parse job description
            if not arc.JobDescription_Parse(descs[i], jobdescs):
                resultDict[i] = [DescriptionParseError("Failed to parse description")]
                continue
            arcdesc = jobdescs[-1]

            # get queue, runtimes and walltime from description
            jobqueue = arcdesc.Resources.QueueName
            if not jobqueue:
                jobqueue = queue
                if v1_0:
                    # set queue in job description
                    arcdesc.Resources.QueueName = queue
            runtimes = [str(env) for env in arcdesc.Resources.RunTimeEnvironment.getSoftwareList()]
            if not runtimes:
                runtimes = []
            walltime = arcdesc.Resources.TotalWallTime.range.max
            if walltime == -1:
                walltime = None

            # do matchmaking
            if matchDescs:
                errors = self.matchJob(ceInfo, jobqueue, runtimes, walltime)
                if errors:
                    resultDict[i] = errors
                    continue

            if v1_0:
                # add delegation ID to description
                arcdesc.DataStaging.DelegationID = delegationID

            # process job description
            if processDescs:
                self._processJobDescription(arcdesc)

            # get input files from description
            inputFiles = self._getArclibInputFiles(arcdesc)

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = arcdesc.UnParse("emies:adl")
            if not unparseResult[0]:
                resultDict[i] = [DescriptionUnparseError("Could not unparse processed description")]
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append((i, inputFiles))

        if not tosubmit:
            return [resultDict[i] for i in range(len(descs))]

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

        # submit jobs to ARC
        # TODO: handle exceptions
        results = self.createJobs(bulkdesc)

        uploadIXs = []  # a list of job indexes for proper result processing
        uploadIDs = []  # a list of jobids for which to upload files
        uploadInputs = []  # a list of job input file dicts for upload

        for (jobix, inputFiles), result in zip(tosubmit, results):
            if isinstance(result, ARCHTTPError):
                resultDict[jobix] = result
            else:
                jobid, state = result
                resultDict[jobix] = (jobid, state)
                uploadIDs.append(jobid)
                uploadInputs.append(inputFiles)
                uploadIXs.append(jobix)

        # upload jobs' local input data
        if uploadData:
            errors = self.uploadJobFiles(uploadIDs, uploadInputs, workers=workers, blocksize=blocksize, timeout=timeout)
            for jobix, uploadErrors in zip(uploadIXs, errors):
                if uploadErrors:
                    jobid, state = resultDict[jobix]
                    resultDict[jobix] = [InputUploadError(jobid, state, uploadErrors)]

        return [resultDict[i] for i in range(len(descs))]

    @classmethod
    def _requestJSONStatic(cls, httpClient, *args, headers={}, **kwargs):
        headers["Accept"] = "application/json"
        resp = httpClient.request(*args, headers=headers, **kwargs)
        text = resp.read().decode()
        return resp.status, text

    @classmethod
    def _uploadTransferWorker(cls, restClient, uploadQueue, errorQueue, logger=getNullLogger()):
        while True:
            try:
                upload = uploadQueue.get(block=False)
            except queue.Empty:
                break
            uploadQueue.task_done()

            if upload.cancelEvent.is_set():
                logger.debug(f"Skipping upload for cancelled job {upload.jobid}")
                continue

            try:
                restClient.uploadFile(upload.url, upload.path)
            except Exception as exc:
                upload.cancelEvent.set()
                errorQueue.put({"jobid": upload.jobid, "error": exc})
                logger.debug(f"Error uploading {upload.path} to {upload.url} for job {upload.jobid}: {exc}")

    @classmethod
    def _downloadTransferWorker(cls, restClient, transferQueue, errorQueue, downloadDir, downloadDict, logger=getNullLogger()):
        while True:
            try:
                transfer = transferQueue.get()
            except TransferQueueEmpty:
                break

            jobid = transfer.jobid
            downloadFiles = downloadDict[jobid]
            if transfer.cancelEvent.is_set():
                logger.debug(f"Skipping download for cancelled job {jobid}")
                continue

            try:
                if transfer.type in ("file", "diagnose"):
                    # download file
                    path = os.path.join(downloadDir, jobid, transfer.path)
                    # TODO: Python >= 3.7
                    #blocksize = restClient.httpClient.conn.blocksize
                    blocksize = None
                    try:
                        restClient.downloadFile(transfer.url, path, blocksize=blocksize)
                    except Exception as exc:
                        error = exc
                        if isinstance(exc, ARCHTTPError):
                            if exc.status == 404:
                                if transfer.type == "diagnose":
                                    error = MissingDiagnoseFile(transfer.url)
                                else:
                                    error = MissingOutputFile(transfer.url)
                        else:
                            transfer.cancelEvent.set()
                        errorQueue.put({"jobid": jobid, "error": error})
                        logger.debug(f"Download {transfer.url} to {path} for job {jobid} failed: {error}")

                    else:
                        logger.debug(f"Download {transfer.url} to {path} for job {jobid} successful")

                elif transfer.type == "listing":
                    # download listing
                    try:
                        listing = restClient.downloadListing(transfer.url)
                    except ARCHTTPError as exc:
                        # work around for invalid output in ARC 6
                        if exc.text == "":
                            listing = {}
                        else:
                            raise
                    except Exception as exc:
                        if not isinstance(exc, ARCHTTPError):
                            transfer.cancelEvent.set()
                        errorQueue.put({"jobid": jobid, "error": exc})
                        logger.debug(f"Download listing {transfer.url} for job {jobid} failed: {exc}")
                    else:
                        # create new transfer jobs
                        restClient._addTransfersFromListing(
                            transferQueue, jobid, downloadFiles, listing, transfer.path, transfer.cancelEvent,
                        )
                        logger.debug(f"Download listing {transfer.url} for job {jobid} successful")

            # every possible exception needs to be handled, otherwise the
            # threads will lock up
            except:
                import traceback
                excstr = traceback.format_exc()
                transfer.cancelEvent.set()
                errorQueue.put({"jobid": jobid, "error": Exception(excstr)})
                logger.debug(f"Download URL {transfer.url} and path {transfer.path} for job {jobid} failed: {excstr}")

    @classmethod
    def _getArclibInputFiles(cls, desc):
        inputFiles = {}
        for infile in desc.DataStaging.InputFiles:
            source = None
            if len(infile.Sources) > 0:
                source = infile.Sources[0].fullstr()
            inputFiles[infile.Name] = source
        return inputFiles

    @classmethod
    def _processJobDescription(cls, jobdesc):
        import arc
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

    @classmethod
    def _parseJobInfo(cls, infoDocument):
        info = {}
        infoDict = infoDocument.get("ComputingActivity", {})

        COPY_KEYS = ["Name", "Type", "LocalIDFromManager", "Owner", "LocalOwner", "StdIn", "StdOut", "StdErr", "LogDir", "Queue"]
        for key in COPY_KEYS:
            if key in infoDict:
                info[key] = infoDict[key]

        INT_KEYS = ["UsedTotalWallTime", "UsedTotalCPUTime", "RequestedTotalWallTime", "RequestedTotalCPUTime", "RequestedSlots", "ExitCode", "WaitingPosition", "UsedMainMemory"]
        for key in INT_KEYS:
            if key in infoDict:
                info[key] = int(infoDict[key])

        TSTAMP_KEYS = ["SubmissionTime", "EndTime", "WorkingAreaEraseTime", "ProxyExpirationTime"]
        for key in TSTAMP_KEYS:
            if key in infoDict:
                info[key] = datetime.datetime.strptime(infoDict[key], "%Y-%m-%dT%H:%M:%SZ")

        VARIABLE_KEYS = ["Error", "ExecutionNode", "RestartState"]
        for key in VARIABLE_KEYS:
            if key in infoDict:
                # /rest/1.0 compatibility
                if isinstance(infoDict[key], list):
                    info[key] = infoDict[key]
                else:
                    info[key] = [infoDict[key]]

        # get state from a list of activity states in different systems
        for state in infoDict.get("State", []):
            if state.startswith("arcrest:"):
                info["State"] = state[len("arcrest:"):]

        # throw out all non ASCII characters from execution node strings
        if "ExecutionNode" in infoDict:
            for i in range(len(info["ExecutionNode"])):
                info["ExecutionNode"][i] = ''.join([i for i in info["ExecutionNode"][i] if ord(i) < 128])

        return info

    ### public static methods ###

    @classmethod
    def getAPIVersionsStatic(cls, httpClient, apiBase="/arex"):
        status, text = cls._requestJSONStatic(httpClient, "GET", f"{apiBase}/rest")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting ARC API versions: {status} {text}")
        apiVersions = json.loads(text)

        # /rest/1.0 compatibility
        if not isinstance(apiVersions["version"], list):
            return [apiVersions["version"]]
        else:
            return apiVersions["version"]

    @classmethod
    def getClient(cls, url=None, host=None, port=None, proxypath=None, logger=getNullLogger(), blocksize=None, timeout=None, version=None, apiBase="/arex"):
        IMPLEMENTED_VERSIONS = {
            "1.0": ARCRest_1_0,
            "1.1": ARCRest_1_1,
        }

        httpClient = HTTPClient(url=url, host=host, port=port, proxypath=proxypath, logger=logger, blocksize=blocksize, timeout=timeout)
        apiVersions = cls.getAPIVersionsStatic(httpClient, apiBase=apiBase)
        if not apiVersions:
            raise ARCError("No supported API versions on CE")

        if version is not None:
            if version not in IMPLEMENTED_VERSIONS:
                raise ARCError(f"No client support for requested API version {version}")
            if version not in apiVersions:
                raise ARCError(f"API version {version} not among CE supported API versions {apiVersions}")
            apiVersion = version
        else:
            apiVersion = None
            for version in reversed(apiVersions):
                if version in IMPLEMENTED_VERSIONS:
                    apiVersion = version
                    break
            if apiVersion is None:
                raise ARCError(f"No client support for CE supported API versions: {apiVersions}")

        logger.debug(f"API version {apiVersion} selected")
        return IMPLEMENTED_VERSIONS[apiVersion](httpClient, apiBase=apiBase, logger=logger)


class ARCRest_1_0(ARCRest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = "1.0"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, description, delegationID=None, queue=None, isADL=True):
        contentType = "application/xml" if isADL else "application/rsl"
        status, text = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs?action=new",
            data=description,
            headers={"Content-Type": contentType},
        )
        if status != 201:
            raise ARCHTTPError(status, text, f"Error submitting jobs: {status} {text}")
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            responses = [jsonData["job"]]
        else:
            responses = jsonData["job"]

        results = []
        for response in responses:
            code, reason = int(response["status-code"]), response["reason"]
            if code != 201:
                results.append(ARCHTTPError(code, reason, f"Submission error: {reason}"))
            else:
                results.append((response["id"], response["state"]))
        return results

    def submitJobs(self, delegationID, descs, queue, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        return self._submitJobs(
            delegationID,
            descs,
            queue,
            processDescs=processDescs,
            matchDescs=matchDescs,
            uploadData=uploadData,
            workers=workers,
            blocksize=blocksize,
            timeout=timeout,
            v1_0=True,
        )


class ARCRest_1_1(ARCRest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = "1.1"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, description, delegationID=None, queue=None, isADL=True):
        params = {"action": "new"}
        if queue:
            params["queue"] = queue
        if delegationID:
            params["delegation_id"] = delegationID
        headers = {"Content-Type": "application/xml" if isADL else "application/rsl"}
        status, text = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs",
            data=description,
            headers=headers,
            params=params,
        )
        if status != 201:
            raise ARCHTTPError(status, text, f"Error submitting jobs: {status} {text}")
        responses = json.loads(text)

        results = []
        for response in responses:
            code, reason = int(response["status-code"]), response["reason"]
            if code != 201:
                results.append(ARCHTTPError(code, reason, f"Submission error: {reason}"))
            else:
                results.append((response["id"], response["state"]))
        return results

    def submitJobs(self, delegationID, descs, queue, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        return self._submitJobs(
            delegationID,
            descs,
            queue,
            processDescs=processDescs,
            matchDescs=matchDescs,
            uploadData=uploadData,
            workers=workers,
            blocksize=blocksize,
            timeout=timeout,
        )


class FileTransfer:

    def __init__(self, jobid, url, path, cancelEvent=None, type=None):
        self.jobid = jobid
        self.url = url
        self.path = path
        self.cancelEvent = cancelEvent
        if self.cancelEvent is None:
            self.cancelEvent = threading.Event()
        self.type = type


class ARCJob:

    def __init__(self, id=None, descstr=None):
        self.id = id
        self.descstr = descstr
        self.name = None
        self.delegid = None
        self.state = None
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


class TransferQueueEmpty(Exception):
    pass


def isLocalInputFile(name, source):
    """
    Return path if local or empty string if remote URL.

    Raises:
        - ValueError: source cannot be parsed
    """
    if not source:
        return name
    url = urlparse(source)
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""
    return url.path
