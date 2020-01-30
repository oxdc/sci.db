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
            feedback: bool = False,
            verbose: bool = True):
    if not is_allowed(source, destination):
        raise AssertionError(f'Migration from {type(source)} to {type(destination)} is not allowed.')
    if confirm and not feedback:
        return
    if isinstance(source, Database):
        migrate_db(source, destination, delete_source, allow_overwrite, confirm, feedback, verbose)
    elif isinstance(source, Bucket):
        migrate_bucket(source, destination, delete_source, allow_overwrite, confirm, feedback, verbose)
    elif isinstance(source, DataSet):
        migrate_data_set(source, destination, delete_source, allow_overwrite, confirm, feedback, verbose)
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
               feedback: bool = False,
               verbose: bool = True):
    if confirm and not feedback:
        return
    for src_bucket in source.all_buckets:
        if not allow_overwrite and (
            destination.get_bucket(src_bucket.name, include_deleted=True)
            or destination.get_bucket(src_bucket.uuid, include_deleted=True)
        ):
            continue
        if verbose:
            print('Migrating:', src_bucket.node_type, src_bucket.name)
        if delete_source:
            src_bucket.move_to(destination)
        else:
            src_bucket.copy_to(destination)


def migrate_bucket(source: Bucket,
                   destination: Union[Database, Bucket, DataSet],
                   delete_source: bool = False,
                   allow_overwrite: bool = False,
                   confirm: bool = True,
                   feedback: bool = False,
                   verbose: bool = True):
    if confirm and not feedback:
        return
    if isinstance(destination, Database):
        if not allow_overwrite and (
            destination.get_bucket(source.name, include_deleted=True)
            or destination.get_bucket(source.uuid, include_deleted=True)
        ):
            return
        if verbose:
            print('Migrating:', source.node_type, source.name)
        if delete_source:
            source.move_to(destination)
        else:
            source.copy_to(destination)
    elif isinstance(destination, Bucket) or isinstance(destination, DataSet):
        if allow_overwrite:
            destination.set_metadata(source.metadata)
            destination.set_properties(source.properties)
        for src_bucket in source.all_data_sets:
            if not allow_overwrite and (
                destination.get_data_set(src_bucket.name, include_deleted=True)
                or destination.get_data_set(src_bucket.uuid, include_deleted=True)
            ):
                continue
            if verbose:
                print('Migrating:', src_bucket.node_type, src_bucket.name)
            if delete_source:
                src_bucket.move_to(destination)
            else:
                src_bucket.copy_to(destination)
    else:
        raise TypeError


def migrate_data_set(source: DataSet,
                     destination: Union[Bucket, DataSet],
                     delete_source: bool = False,
                     allow_overwrite: bool = False,
                     confirm: bool = True,
                     feedback: bool = False,
                     verbose: bool = True):
    if confirm and not feedback:
        return
    if not allow_overwrite and (
        destination.get_data_set(source.name, include_deleted=True)
        or destination.get_data_set(source.uuid, include_deleted=True)
    ):
        return
    if verbose:
        print('Migrating:', source.node_type, source.name)
    if delete_source:
        source.move_to(destination)
    else:
        source.copy_to(destination)


def migrate_data(source: Data,
                 destination: Union[DataSet, Data],
                 delete_source: bool = False,
                 allow_overwrite: bool = False,
                 confirm: bool = True,
                 feedback: bool = False,
                 verbose: bool = True):
    if confirm and not feedback:
        return
    if isinstance(destination, DataSet):
        if not allow_overwrite and destination.get_data(source.name):
            return
        if verbose:
            print('Migrating:', 'Data', source.name)
        data = destination.add_data(source.name)
        data.import_file(source.path, confirm, feedback)
        data.set_properties(source.properties)
        data.set_metadata(source.metadata)
        if delete_source:
            source.parent.delete_data(source.name, confirm, feedback)
    elif isinstance(destination, Data):
        if not allow_overwrite:
            return
        if verbose:
            print('Migrating:', 'Data', source.name)
        destination.import_file(source.path)
        destination.set_properties(source.properties)
        destination.set_metadata(source.metadata)
        if delete_source:
            source.parent.delete_data(source.name, confirm, feedback)
    else:
        raise TypeError
