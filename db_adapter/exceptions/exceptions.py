from pprint import PrettyPrinter


class DataLayerError(Exception):
    """
    Base class for data-layer errors.
    """

    def __init__(self):
        self.printer = PrettyPrinter(width=40, compact=True)


class InconsistencyError(DataLayerError):
    def __init__(self, message, error_context):
        self.message = message
        self.error_context = error_context

    def __repr__(self):
        return self.message + ", " + self.printer.pprint(self.error_context)


class NoTimeseriesFound(DataLayerError):
    def __init__(self, message, error_context):
        self.message = message
        self.error_context = error_context

    def __repr__(self):
        return self.message + ", " + self.printer.pprint(self.error_context)
