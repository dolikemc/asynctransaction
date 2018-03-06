from enum import Enum


class State(Enum):
    BadRequest = (400, 'request body unsupported', '')
    BadRequestJsonDecode = (400, 'body could not be decoded as json', '')
    BadRequestMandatoryKey = (400, 'mandatory key(s) are missing in the body', '')
    BadRequestNotStoreAble = (400, 'data are not store able ', '')
    BadRequestDBError = (400, 'data base error', '')
    RequestReceived = (200, 'received', '')
    RequestStored = (201, 'stored', '')
    ConflictRequest = (409, 'already stored', '')
    New = (1, 'new', '')
    InProgress = (2, 'in progress', '')
    Published = (3, 'published', '')
    Processed = (4, 'processed', '')
    Error = (5, 'error', '')

    def __init__(self, code, message, detail):
        self._code = code
        self._message = message
        self._detail = detail

    @property
    def code(self):
        return self._code

    @property
    def reason(self):
        return self._message

    @property
    def message(self):
        return ':'.join([self._message, self._detail])
