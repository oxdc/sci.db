from scidb.plugins.backup.base.backend import BackupBackend
from scidb.utils.extractor import db_to_json, recover_db
from scidb.utils.iteration import iter_data
from scidb.core import Database, Data
from ..base.backup_profile import BackupProfile
from typing import Tuple, List, Union
from pathlib import Path
from datetime import datetime
import json
import shutil


class LocalBackupProfile(BackupProfile):
    def __init__(self,
                 profile_name: Union[None, str] = None,
                 time: Union[None, datetime] = None,
                 path: Union[None, str, Path] = None):
        self.__root_path__ = None
        self.set_path(path)
        super().__init__(profile_name, time)

    @property
    def path(self) -> Path:
        return self.__root_path__

    @property
    def db_json(self) -> Path:
        return self.__root_path__ / self.name

    @property
    def obj_path(self) -> Path:
        return self.__root_path__ / 'objects'

    def set_path(self, path: Union[None, str, Path]):
        if path is None or isinstance(path, Path):
            self.__root_path__ = path
        else:
            self.__root_path__ = Path(path)


class LocalBackend(BackupBackend):
    def __init__(self, db_name: str, db_path: Union[str, Path], backup_path: Union[str, Path]):
        self.__db_name__ = db_name
        self.__db_path__ = db_path if isinstance(db_path, Path) else Path(db_path)
        self.__backup_path__ = backup_path if isinstance(backup_path, Path) else Path(backup_path)
        self.__db__ = Database(db_name, str(db_path))
        super().__init__()

    def ping(self) -> Union[bool, Tuple[bool, float]]:
        return True

    def connect(self):
        self.__is_connected__ = True

    def create_backup(self, verbose: bool = True):
        profile = LocalBackupProfile(time=datetime.now(), path=self.__backup_path__)
        profile.obj_path.mkdir(parents=True, exist_ok=True)
        with open(str(profile.db_json), 'w') as fp:
            json.dump(
                obj=db_to_json(self.__db_name__, self.__db_path__),
                fp=fp,
                indent=2
            )

        def copy_data_objs(data: Data):
            if verbose:
                print('Added:', data.name, data.path)
            dst_path = profile.obj_path / data.sha1()
            if not dst_path.exists():
                shutil.copyfile(
                    src=str(data.path),
                    dst=str(dst_path)
                )

        for bucket in self.__db__.all_buckets:
            iter_data(bucket, copy_data_objs, include_deleted=True)

    def sync_backup(self):
        pass

    def list_backups(self) -> List[LocalBackupProfile]:
        backups = [child for child in self.__backup_path__.glob('db_backup_*.json') if child.is_file()]
        return [LocalBackupProfile(profile_name=backup.name, path=self.__backup_path__) for backup in backups]

    def fetch_backup(self, time: datetime) -> Union[None, LocalBackupProfile]:
        profile = LocalBackupProfile(time=time, path=self.__backup_path__)
        if profile.db_json.exists():
            return profile
        else:
            return None

    def recover_from_backup(self, profile: LocalBackupProfile, new_path: Union[str, Path]):
        if isinstance(new_path, str):
            new_path = Path(new_path)
        if new_path.exists():
            raise FileExistsError

        def get_file(sha1: str) -> str:
            return str(profile.obj_path / sha1)

        with open(str(profile.db_json)) as fp:
            db_json = json.load(fp)
            recover_db(db_json, new_path, get_file=get_file)
