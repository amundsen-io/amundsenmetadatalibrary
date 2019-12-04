
class NotFoundException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BadgeNotInWhitelistException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
