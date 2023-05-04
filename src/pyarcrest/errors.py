class X509Error(Exception):
    pass


class HTTPClientError(Exception):
    pass


class ARCError(Exception):

    def __init__(self, msg=""):
        self.msg = msg

    def __str__(self):
        return self.msg


class ARCHTTPError(ARCError):

    def __init__(self, status, text, msg=""):
        super().__init__(msg)
        self.status = status
        self.text = text


class DescriptionParseError(ARCError):
    pass


class DescriptionUnparseError(ARCError):
    pass


class InputFileError(ARCError):
    pass


class NoValueInARCResult(ARCError):
    pass

class MatchmakingError(ARCError):
    pass


class MissingResultFile(ARCError):

    def __init__(self, filename):
        self.filename = filename


class MissingOutputFile(MissingResultFile):

    def __str__(self):
        return f"Missing output file {self.filename}"


class MissingDiagnoseFile(MissingResultFile):

    def __str__(self):
        return f"Missing diagnose file {self.filename}"
