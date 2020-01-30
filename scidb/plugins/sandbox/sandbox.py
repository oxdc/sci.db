from scidb.core import Database, Bucket, DataSet
from scidb.utils.migrator import migrate
from typing import Set, Union


class SandboxManager:
    def __init__(self, db: Database):
        self.__db__ = db

    @property
    def db(self) -> Database:
        return self.__db__

    @classmethod
    def __convert_name__(cls,
                         sandbox_name: Union[None, str] = None,
                         pure_name: Union[None, str] = None) -> str:
        if sandbox_name is not None:
            return sandbox_name.replace('__SANDBOX_', '').replace('__', '')
        elif pure_name is not None:
            return f'__SANDBOX_{pure_name}__'
        else:
            raise AssertionError

    @classmethod
    def is_sandbox(cls, bucket: Bucket) -> bool:
        return bucket.name.startswith('__SANDBOX_') and bucket.name.endswith('__')

    def create_sandbox(self, name: str) -> Bucket:
        sandbox_name = self.__convert_name__(pure_name=name)
        return self.__db__.add_bucket(sandbox_name)

    def get_sandbox(self, name: str, include_deleted: bool = False) -> Union[Bucket, None]:
        sandbox_name = self.__convert_name__(pure_name=name)
        sandbox = self.__db__.get_bucket(sandbox_name, include_deleted)
        return sandbox

    @property
    def sandboxes(self) -> Set[Bucket]:
        return set(filter(lambda bucket: self.is_sandbox(bucket), self.__db__.all_buckets))

    def list_sandbox(self) -> Set[Bucket]:
        return self.sandboxes

    def clear_sandbox(self, name: str, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        self.delete_sandbox(name, conform, feedback)
        self.create_sandbox(name)

    def delete_sandbox(self, name: str, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        sandbox = self.get_sandbox(name, include_deleted=True)
        if sandbox is None:
            raise FileNotFoundError
        else:
            sandbox.delete()
            self.__db__.clear_trash(conform, feedback)

    def delete_all_sandboxes(self, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        for sandbox in self.sandboxes:
            sandbox.delete()
            sandbox.clear_trash(conform, feedback)

    def migrate_sandbox(self,
                        name: str,
                        destination: Union[Bucket, DataSet],
                        delete_source: bool = True,
                        allow_overwrite: bool = False,
                        conform: bool = True,
                        feedback: bool = False,
                        verbose: bool = True):
        if conform and not feedback:
            return
        sandbox = self.get_sandbox(name, include_deleted=True)
        if sandbox is None:
            raise FileNotFoundError
        for data_set in sandbox.all_data_sets:
            migrate(data_set, destination, delete_source, allow_overwrite, conform, feedback, verbose)
