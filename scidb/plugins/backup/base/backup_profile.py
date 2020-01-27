from datetime import datetime
from typing import Union


class BackupProfile:
    def __init__(self, profile_name: Union[None, str] = None, time: Union[None, datetime] = None, **kwargs):
        self.__profile_name__ = profile_name
        self.__time__ = time
        self.parse_name()

    def parse_name(self):
        if self.__profile_name__ is not None:
            time_str = self.__profile_name__.replace('db_backup_', '').replace('.json', '')
            self.__time__ = datetime.strptime(time_str, '%Y%m%d-%H%M%S-%f')
        elif self.__time__ is not None:
            self.__profile_name__ = f"db_backup_{self.__time__.strftime('%Y%m%d-%H%M%S-%f')}.json"
        else:
            raise AssertionError

    @property
    def name(self) -> str:
        return f"db_backup_{self.__time__.strftime('%Y%m%d-%H%M%S-%f')}.json"

    @property
    def time(self) -> datetime:
        return self.__time__
