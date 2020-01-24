from typing import Tuple, Union


class BackupBackend:
    def __init__(self, **kwargs):
        self.__is_connected__ = False

    def ping(self) -> Union[bool, Tuple[bool, float]]:
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        return self.__is_connected__

    def create_backup(self, **kwargs):
        raise NotImplementedError

    def sync_backup(self, **kwargs):
        raise NotImplementedError

    def list_backups(self, **kwargs):
        raise NotImplementedError

    def fetch_backup(self, **kwargs):
        raise NotImplementedError
