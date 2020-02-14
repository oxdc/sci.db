from ..base.backend import BackupBackend
from ..base.backup_profile import BackupProfile
from scidb.core import Database, Data
from scidb.utils.extractor import db_to_json, recover_db
from scidb.utils.iteration import iter_data
from typing import Tuple, List, Union
from pathlib import Path
from datetime import datetime
from minio import Minio
from minio.error import NoSuchKey, InvalidBucketError, NoSuchBucket
from urllib3.poolmanager import PoolManager
from tempfile import TemporaryDirectory
import json
import shutil


class MinioBackupProfile(BackupProfile):
    def __init__(self,
                 db_name: str,
                 profile_name: Union[None, str] = None,
                 time: Union[None, datetime] = None,
                 obj_bucket_name: str = 'scidb-objects',
                 backup_bucket_name: str = 'scidb-backups'):
        self.obj_bucket_name = obj_bucket_name
        self.backup_bucket_name = backup_bucket_name
        self.__temp_dir__ = TemporaryDirectory()
        self.__temp_path__ = Path(self.__temp_dir__.name)
        self.obj_list = dict()
        super().__init__(db_name, profile_name, time)

    @property
    def temp_path(self) -> Path:
        return self.__temp_path__

    @property
    def db_json(self) -> Path:
        return self.__temp_path__ / self.name

    def remove_temp(self):
        self.__temp_dir__.cleanup()

    def __str__(self) -> str:
        return f"MinioBackupProfile(" \
               f"db='{self.__db_name__}', " \
               f"time='{self.time}', " \
               f"obj_bucket='{self.obj_bucket_name}', " \
               f"backup_bucket='{self.backup_bucket_name}')"

    def __repr__(self) -> str:
        return f"MinioBackupProfile(" \
               f"db='{self.__db_name__}', " \
               f"time='{self.time}', " \
               f"obj_bucket='{self.obj_bucket_name}', " \
               f"backup_bucket='{self.backup_bucket_name}')"


class MinioBackend(BackupBackend):
    def __init__(self,
                 db_name: str,
                 db_path: Union[str, Path],
                 endpoint: str,
                 access_key: str,
                 secret_key: str,
                 secure: bool = True,
                 region: Union[str, None] = None,
                 http_client: Union[PoolManager, None] = None,
                 obj_bucket_name: str = 'scidb-objects',
                 backup_bucket_name: str = 'scidb-backups'):
        self.__db_name__ = db_name
        self.__db_path__ = db_path if isinstance(db_path, Path) else Path(db_path)
        self.__endpoint__ = endpoint
        self.__access_key__ = access_key
        self.__secret_key__ = secret_key
        self.__secure__ = secure
        self.__region__ = region
        self.__http_client__ = http_client
        self.obj_bucket_name = obj_bucket_name
        self.backup_bucket_name = backup_bucket_name
        self.__server__ = Minio(
            endpoint,
            access_key,
            secret_key,
            secure=secure,
            region=region,
            http_client=http_client
        )
        self.__db__ = Database(db_name, str(db_path))
        self.__current_profile__: Union[None, MinioBackupProfile] = None
        super().__init__()

    @property
    def server(self):
        return self.__server__

    def init_remote_storage(self) -> bool:
        assert self.__current_profile__ is not None
        if not self.__server__.bucket_exists(self.__current_profile__.obj_bucket_name):
            self.__server__.make_bucket(self.__current_profile__.obj_bucket_name)
        if not self.__server__.bucket_exists(self.__current_profile__.backup_bucket_name):
            self.__server__.make_bucket(self.__current_profile__.backup_bucket_name)
        return self.__server__.bucket_exists(self.__current_profile__.obj_bucket_name) \
            and self.__server__.bucket_exists(self.__current_profile__.backup_bucket_name)

    def ping(self) -> Union[bool, Tuple[bool, float]]:
        return True

    def connect(self):
        self.__is_connected__ = True

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

    def create_backup(self, verbose: bool = True, require_hash_update: bool = False) -> MinioBackupProfile:
        profile = MinioBackupProfile(
            db_name=self.__db_name__,
            time=datetime.now(),
            backup_bucket_name=self.backup_bucket_name,
            obj_bucket_name=self.obj_bucket_name
        )
        with open(str(profile.db_json), 'w') as fp:
            json.dump(
                obj=db_to_json(self.__db_name__, self.__db_path__),
                fp=fp,
                indent=2
            )

        def list_data_objs(data: Data):
            if verbose:
                print('Added:', data.name, data.path)
            h = data.sha1(require_update=require_hash_update)
            if h not in profile.obj_list and not self.exists_object(profile.obj_bucket_name, h):
                profile.obj_list[h] = {
                    'path': data.path,
                    'metadata': data.metadata
                }

        for bucket in self.__db__.all_buckets:
            iter_data(bucket, list_data_objs, include_deleted=True, )

        self.__current_profile__ = profile
        return profile

    def sync_backup(self, verbose: bool = True):
        if self.__current_profile__ is None:
            raise AssertionError('Backup has not been created.')
        self.init_remote_storage()
        self.__server__.fput_object(
            self.__current_profile__.backup_bucket_name,
            self.__current_profile__.name,
            str(self.__current_profile__.db_json)
        )
        for name, info in self.__current_profile__.obj_list.items():
            if verbose:
                print('Sync:', name)
            self.__server__.fput_object(
                self.__current_profile__.obj_bucket_name,
                name,
                info['path'],
                metadata=info['metadata']
            )

    def list_backups(self, db_name: Union[None, str] = None) -> List[MinioBackupProfile]:
        if db_name is None:
            db_name = self.__db_name__
        if not self.__server__.bucket_exists(self.backup_bucket_name):
            return []
        else:
            return [
                MinioBackupProfile(
                    db_name=db_name,
                    profile_name=backup.object_name,
                    obj_bucket_name=self.obj_bucket_name,
                    backup_bucket_name=self.backup_bucket_name
                )
                for backup in self.__server__.list_objects(self.backup_bucket_name)
            ]

    def fetch_backup(self, time: datetime, db_name: Union[None, str] = None) -> Union[None, MinioBackupProfile]:
        if db_name is None:
            db_name = self.__db_name__
        profile = MinioBackupProfile(
            db_name=db_name,
            time=time,
            obj_bucket_name=self.obj_bucket_name,
            backup_bucket_name=self.backup_bucket_name
        )
        if self.exists_object(self.backup_bucket_name, profile.name):
            return profile
        else:
            return None

    def recover_from_backup(self, profile: MinioBackupProfile, new_path: Union[str, Path]):
        if isinstance(new_path, str):
            new_path = Path(new_path)
        if new_path.exists():
            raise FileExistsError
        self.__server__.fget_object(
            profile.backup_bucket_name,
            profile.name,
            str(profile.db_json)
        )

        def get_file(sha1: str) -> str:
            file_path = str(profile.temp_path / sha1)
            self.__server__.fget_object(
                profile.obj_bucket_name,
                sha1,
                file_path
            )
            return file_path

        with open(str(profile.db_json)) as fp:
            db_json = json.load(fp)
            recover_db(db_json, new_path, get_file=get_file)

        profile.remove_temp()
