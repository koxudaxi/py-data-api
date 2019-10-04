class DataAPIError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


class MultipleResultsFound(DataAPIError):
    def __init__(self) -> None:
        super().__init__('Multiple rows were found for one()')


class NoResultFound(DataAPIError):
    def __init__(self) -> None:
        super().__init__('No row was found for one()')
