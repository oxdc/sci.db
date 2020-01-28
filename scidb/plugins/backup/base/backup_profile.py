from datetime import datetime
from typing import Union


class BackupProfile:
    def __init__(self,
                 db_name: str,
                 profile_name: Union[None, str] = None,
                 time: Union[None, datetime] = None,
                 **kwargs):
        self.__db_name__ = db_name
        self.__profile_name__ = profile_name
        self.__time__ = time
        self.parse_name()

    def parse_name(self):
        if self.__profile_name__ is not None:
            time_str = self.__profile_name__.replace(f'db_backup_{self.__db_name__}_', '').replace('.json', '')
            self.__time__ = datetime.strptime(time_str, '%Y%m%d-%H%M%S-%f')
        elif self.__time__ is not None:
            self.__profile_name__ = f"db_backup_{self.__db_name__}_{self.__time__.strftime('%Y%m%d-%H%M%S-%f')}.json"
        else:
            raise AssertionError

    @property
    def db_name(self) -> str:
        return self.__db_name__

    def rename_db(self, new_db_name: str):
        self.__db_name__ = new_db_name
        self.__profile_name__ = self.name

    @property
    def name(self) -> str:
        return f"db_backup_{self.__db_name__}_{self.__time__.strftime('%Y%m%d-%H%M%S-%f')}.json"

    @property
    def time(self) -> datetime:
        return self.__time__

    def __str__(self) -> str:
        return f"GenericBackupProfile(db='{self.__db_name__}', time='{self.time}')"

    def __repr__(self) -> str:
        return f"GenericBackupProfile(db='{self.__db_name__}', time='{self.time}')"
