from .low.node import Root
from .bucket import Bucket
from typing import Set


class Database(Root):
    def __init__(self, name: str, path: str, version: str = 'alpha1'):
        self.__db_name__ = name
        self.__db_version__ = version
        self.__buckets__ = set()
        super().__init__(path)
        self.init_buckets()

    def init_buckets(self):
        children = filter(lambda child: child.is_dir(), self.path.iterdir())
        for bucket in children:
            self.__buckets__.add(Bucket(bucket.name, self))

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
        if self.get_bucket(name) is not None:
            raise FileExistsError
        new_bucket = Bucket(bucket_name=name, parent=self)
        self.__buckets__.add(new_bucket)
        return new_bucket

    def get_bucket(self, name_or_uuid: str, include_deleted: bool = False) -> Bucket:
        target = None
        search_list = self.all_buckets if include_deleted else self.buckets
        for bucket in search_list:
            if bucket.name == name_or_uuid or bucket.uuid == name_or_uuid:
                target = bucket
        return target

    def clear_trash(self, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        for bucket in self.trash:
            bucket.purge_storage(conform, feedback)
        for bucket in self.buckets:
            bucket.clear_trash(conform, feedback)
        self.__buckets__ = set(self.buckets)

    def init_storage(self):
        try:
            super().init_storage()
        except FileExistsError:
            raise FileExistsError('The directory contains files.')
