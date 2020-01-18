from typing import Tuple


class BackupBackend:
    def __init__(self, **kwargs):
        self.__is_connected__ = False

    def ping(self) -> [bool, Tuple[bool, float]]:
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        return self.__is_connected__

    def create_backup(self):
        raise NotImplementedError

    def sync_backup(self):
        raise NotImplementedError

    def fetch_backup(self):
        raise NotImplementedError
