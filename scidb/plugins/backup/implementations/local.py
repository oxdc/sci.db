from scidb.plugins.backup.base.backend import BackupBackend
from scidb.utils.extractor import db_to_json
from scidb.utils.iteration import iter_data
from scidb.core import Database, Data
from typing import Tuple, List, Union
from pathlib import Path
from datetime import datetime
import json
import shutil


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

    def create_backup(self):
        db_json = self.__backup_path__ / f"db_backup_{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        obj_path = self.__backup_path__ / 'objects'
        obj_path.mkdir(parents=True, exist_ok=True)
        with open(str(db_json), 'w') as fp:
            json.dump(
                obj=db_to_json(self.__db_name__, self.__db_path__),
                fp=fp,
                indent=2
            )

        def copy_data_objs(data: Data, root_path: Path = obj_path):
            print(data.name, data.path)
            dst_path = root_path / data.sha1()
            if not dst_path.exists():
                shutil.copyfile(
                    src=str(data.path),
                    dst=str(dst_path)
                )

        for bucket in self.__db__.all_buckets:
            iter_data(bucket, copy_data_objs, include_deleted=True, root_path=obj_path)

    def sync_backup(self):
        pass

    def list_backups(self) -> List[Tuple[str, Path]]:
        backups = [child for child in self.__backup_path__.glob('*') if child.is_dir()]
        return [(backup.name, backup) for backup in backups]

    def fetch_backup(self, time: datetime) -> Tuple[str, Path]:
        backup_path = self.__backup_path__ / f"db_backup_{time.strftime('%Y%m%d-%H%M%S')}"
        if backup_path.exists():
            return backup_path.name, backup_path
