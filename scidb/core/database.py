from .low.node import Root
from .bucket import Bucket
from typing import Set
from time import process_time


class Database(Root):
    def __init__(self, name: str, path: str, version: str = 'alpha1', verbose: bool = True):
        if verbose:
            print(f"[SciDB {version}] Loading database '{name}' @ {path} ...")
        start = process_time()
        self.__db_name__ = name
        self.__db_version__ = version
        self.__buckets__ = set()
        self.__bucket_count__ = 0
        self.__data_set_count__ = 0
        self.__data_count__ = 0
        self.__protected_methods__ = {
            'increase_bucket_count': self.__increase_bucket_count__,
            'increase_data_set_count': self.__increase_data_set_count__,
            'increase_data_count': self.__increase_data_count__
        }
        super().__init__(path)
        self.init_storage()
        self.init_buckets()
        end = process_time()
        if verbose:
            print(f"[SciDB {version}] Loaded database '{name}' "
                  f"with {self.__bucket_count__} buckets, "
                  f"{self.__data_set_count__} data sets "
                  f"and {self.__data_count__} data files "
                  f"in {end - start: .2f} seconds.")

    @property
    def bucket_count(self):
        return self.__bucket_count__

    def __increase_bucket_count__(self):
        self.__bucket_count__ += 1

    @property
    def data_set_count(self):
        return self.__data_set_count__

    def __increase_data_set_count__(self):
        self.__data_set_count__ += 1

    @property
    def data_count(self):
        return self.__data_count__

    def __increase_data_count__(self):
        self.__data_count__ += 1

    def init_buckets(self):
        children = filter(lambda child: child.is_dir(), self.path.iterdir())
        for bucket in children:
            self.__buckets__.add(Bucket(bucket.name, self, protected_parent_methods=self.__protected_methods__))

    @property
    def name(self) -> str:
        return self.__db_name__

    def rename(self, new_name: str):
        self.__db_name__ = new_name

    @property
    def version(self) -> str:
        return self.__db_version__

    @property
    def buckets(self) -> Set[Bucket]:
        return set(filter(lambda bucket: not bucket.deleted, self.__buckets__))

    @property
    def trash(self) -> Set[Bucket]:
        return set(filter(lambda bucket: bucket.deleted, self.__buckets__))

    @property
    def all_buckets(self) -> Set[Bucket]:
        return self.__buckets__

    def add_bucket(self, name: str) -> Bucket:
        if self.get_bucket(name, include_deleted=True) is not None:
            raise FileExistsError
        new_bucket = Bucket(bucket_name=name, parent=self, protected_parent_methods=self.__protected_methods__)
        self.__buckets__.add(new_bucket)
        return new_bucket

    def insert_bucket(self, bucket: Bucket) -> Bucket:
        if bucket in self.__buckets__ \
                or self.get_bucket(bucket.name, include_deleted=True) is not None \
                or self.get_bucket(bucket.uuid, include_deleted=True):
            raise FileExistsError
        if bucket.parent is not self:
            raise AssertionError
        self.__buckets__.add(bucket)
        return bucket

    def get_bucket(self, name_or_uuid: str, include_deleted: bool = False) -> Bucket:
        target = None
        search_list = self.all_buckets if include_deleted else self.buckets
        for bucket in search_list:
            if bucket.name == name_or_uuid or bucket.uuid == name_or_uuid:
                target = bucket
        return target

    def touch_bucket(self, name: str) -> Bucket:
        target = self.get_bucket(name, include_deleted=True)
        if target is None:
            target = self.add_bucket(name)
        if target.deleted:
            raise AssertionError
        return target

    def clear_trash(self, confirm: bool = True, feedback: bool = False):
        if confirm and not feedback:
            return
        for bucket in self.trash:
            bucket.purge_storage(confirm, feedback)
        for bucket in self.buckets:
            bucket.clear_trash(confirm, feedback)
        self.__buckets__ = set(self.buckets)

    def init_storage(self):
        if not self.path.exists():
            super().init_storage()
