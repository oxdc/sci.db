# Migrate
# database --> database
# bucket --> database
# bucket --> bucket
# bucket --> dataset
# dataset --> bucket
# dataset --> dataset
# data --> dataset
# data --> data

from scidb.core import Database, Bucket, DataSet, Data
from typing import Union


ALLOWED_MIGRATION = [
    (Database, Database),
    (Bucket, Database),
    (Bucket, Bucket),
    (Bucket, DataSet),
    (DataSet, Bucket),
    (DataSet, DataSet),
    (Data, DataSet),
    (Data, Data)
]


def migrate(source: Union[Database, Bucket, DataSet, Data],
            destination: Union[Database, Bucket, DataSet, Data],
            delete_source: bool = False,
            allow_overwrite: bool = False,
            confirm: bool = True,
            feedback: bool = False):
    if not is_allowed(source, destination):
        raise AssertionError(f'Migration from {type(source)} to {type(destination)} is not allowed.')
    if confirm and not feedback:
        return
    if isinstance(source, Database):
        migrate_db(source, destination, delete_source, allow_overwrite, confirm, feedback)
    elif isinstance(source, Bucket):
        migrate_bucket(source, destination, delete_source, allow_overwrite, confirm, feedback)
    elif isinstance(source, DataSet):
        migrate_data_set(source, destination, delete_source, allow_overwrite, confirm, feedback)
    elif isinstance(source, Data):
        migrate_data(source, destination, delete_source, allow_overwrite, confirm, feedback)
    else:
        raise TypeError


def is_allowed(source: Union[Database, Bucket, DataSet, Data],
               destination: Union[Database, Bucket, DataSet, Data]):
    src_dst_types = type(source), type(destination)
    return src_dst_types in ALLOWED_MIGRATION


def migrate_db(source: Database,
               destination: Database,
               delete_source: bool = False,
               allow_overwrite: bool = False,
               confirm: bool = True,
               feedback: bool = False):
    if confirm and not feedback:
        return
    for src_bucket in source.all_buckets:
        if not allow_overwrite and (
                destination.get_bucket(src_bucket.name) or destination.get_bucket(src_bucket.uuid)
        ):
            return
        if delete_source:
            src_bucket.move_to(destination)
        else:
            src_bucket.copy_to(destination)


def migrate_bucket(source: Bucket,
                   destination: Union[Database, Bucket, DataSet],
                   delete_source: bool = False,
                   allow_overwrite: bool = False,
                   confirm: bool = True,
                   feedback: bool = False):
    if confirm and not feedback:
        return
    raise NotImplementedError


def migrate_data_set(source: DataSet,
                     destination: Union[Bucket, DataSet],
                     delete_source: bool = False,
                     allow_overwrite: bool = False,
                     confirm: bool = True,
                     feedback: bool = False):
    if confirm and not feedback:
        return
    raise NotImplementedError


def migrate_data(source: Data,
                 destination: Union[DataSet, Data],
                 delete_source: bool = False,
                 allow_overwrite: bool = False,
                 confirm: bool = True,
                 feedback: bool = False):
    if confirm and not feedback:
        return
    raise NotImplementedError
