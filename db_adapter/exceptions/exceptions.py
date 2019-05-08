from pprint import PrettyPrinter


class AdapterError(Exception):
    """
    Curw Base class for data-layer errors.
    """

    def __init__(self):
        self.printer = PrettyPrinter(width=40, compact=True)

    pass


class InconsistencyError(AdapterError):
    def __init__(self, message, error_context):
        self.message = message
        self.error_context = error_context

    def __repr__(self):
        return self.message + ", " + self.printer.pprint(self.error_context)


class NoTimeseriesFound(AdapterError):
    def __init__(self, message, error_context):
        self.message = message
        self.error_context = error_context

    def __repr__(self):
        return self.message + ", " + self.printer.pprint(self.error_context)


class DatabaseAdapterError(AdapterError):
    def __init__(self, message):
        self.message = message

    def __init__(self, message, exception):
        self.message = message
        self.exception = exception


class DuplicateEntryError(AdapterError):
    def __init__(self, message, error_context):
        self.message = message
        self.error_context = error_context

    def __repr__(self):
        return self.message + ", " + self.printer.pprint(self.error_context)