from scidb.plugins.backup.base.backend import BackupBackend
from scidb.utils.extractor import db_to_json
from scidb.utils.iteration import iter_data
from scidb.core import Database, Data
from typing import Tuple, List, Union
from pathlib import Path
from datetime import datetime
from minio import Minio
from minio.error import NoSuchKey, InvalidBucketError, NoSuchBucket
from urllib3.poolmanager import PoolManager
from tempfile import TemporaryDirectory
import json
import shutil


class MinioBackend(BackupBackend):
    def __init__(self,
                 db_name: str,
                 db_path: Union[str, Path],
                 endpoint: str,
                 access_key: str,
                 secret_key: str,
                 secure: bool = True,
                 region: Union[str, None] = None,
                 http_client: Union[PoolManager, None] = None):
        self.__db_name__ = db_name
        self.__db_path__ = db_path if isinstance(db_path, Path) else Path(db_path)
        self.__endpoint__ = endpoint
        self.__access_key__ = access_key
        self.__secret_key__ = secret_key
        self.__secure__ = secure
        self.__region__ = region
        self.__http_client__ = http_client
        self.__server__ = Minio(
            endpoint,
            access_key,
            secret_key,
            secure=secure,
            region=region,
            http_client=http_client
        )
        self.__db__ = Database(db_name, str(db_path))
        self.__temp_dir__ = TemporaryDirectory()
        self.__temp_path__ = Path(self.__temp_dir__.name)
        self.__obj_list__ = dict()
        self.__backup_name__ = ''
        super().__init__()

    @property
    def server(self):
        return self.__server__

    def init_remote_storage(self) -> bool:
        if not self.__server__.bucket_exists('scidb-objects'):
            self.__server__.make_bucket('scidb-objects')
        if not self.__server__.bucket_exists('scidb-backups'):
            self.__server__.make_bucket('scidb-backups')
        return self.__server__.bucket_exists('scidb-objects') and self.__server__.bucket_exists('scidb-backups')

    def ping(self) -> Union[bool, Tuple[bool, float]]:
        return True

    def connect(self):
        self.__is_connected__ = True

    def __init_local_temp__(self):
        if self.__temp_path__.exists():
            shutil.rmtree(str(self.__temp_path__), ignore_errors=True)
        self.__temp_path__.mkdir(parents=True)

    def exists_object(self, bucket_name: str, object_name: str) -> bool:
        try:
            self.__server__.remove_incomplete_upload(bucket_name, object_name)
            return self.__server__.stat_object(bucket_name, object_name) is not None
        except NoSuchKey:
            return False
        except InvalidBucketError:
            return False
        except NoSuchBucket:
            return False

    def create_backup(self):
        self.__backup_name__ = f"db_backup_{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        db_json = self.__temp_path__ / self.__backup_name__
        with open(str(db_json), 'w') as fp:
            json.dump(
                obj=db_to_json(self.__db_name__, self.__db_path__),
                fp=fp,
                indent=2
            )

        def list_data_objs(data: Data, results: dict):
            h = data.sha1()
            if h not in results and not self.exists_object('scidb-objects', h):
                results[h] = {
                    'path': data.path,
                    'metadata': data.metadata
                }
            print('Added:', data.name, data.path)

        self.__obj_list__ = dict()
        for bucket in self.__db__.all_buckets:
            iter_data(bucket, list_data_objs, include_deleted=True, results=self.__obj_list__)

    def sync_backup(self):
        self.init_remote_storage()
        self.__server__.fput_object('scidb-backups', self.__backup_name__, self.__temp_path__ / self.__backup_name__)
        for name, info in self.__obj_list__.items():
            print('Sync:', name)
            self.__server__.fput_object('scidb-objects', name, info['path'], metadata=info['metadata'])
        self.__backup_name__ = ''

    def list_backups(self) -> List[str]:
        if not self.__server__.bucket_exists('scidb-backups'):
            return []
        return [str(backup.object_name) for backup in self.__server__.list_objects('scidb-backups')]

    def fetch_backup(self, time: datetime) -> Union[str, None]:
        backup_name = f"db_backup_{time.strftime('%Y%m%d-%H%M%S')}.json"
        if self.exists_object('scidb-backups', backup_name):
            return backup_name
        else:
            return None
