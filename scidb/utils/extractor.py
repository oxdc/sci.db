from scidb.core import Database, Bucket, DataSet, Data
from typing import Union, Callable
from json import loads
from pathlib import Path
from tempfile import TemporaryDirectory


def db_to_json(db_name: str, db_path: str, verbose: bool = True, require_hash_update: bool = False):
    db = Database(db_name, db_path)
    if verbose:
        print('Extract Database:', db_name, '@', db_path)
    results = dict()
    results['properties'] = dict()
    results['properties']['name'] = db.name
    results['properties']['path'] = str(db.path)
    results['properties']['version'] = db.version
    results['buckets'] = dict()
    for bucket in db.all_buckets:
        results['buckets'][bucket.name] = dict()
        results['buckets'][bucket.name]['properties'] = bucket.properties.data.to_dict()
        results['buckets'][bucket.name]['metadata'] = bucket.metadata.data.to_dict()
        results['buckets'][bucket.name]['children'] = dict()
        bucket_to_json(bucket,
                       results=results['buckets'][bucket.name]['children'],
                       verbose=verbose,
                       require_hash_update=require_hash_update)
    return results


def bucket_to_json(bucket: Bucket, results: dict, verbose: bool = True, require_hash_update: bool = False):
    if verbose:
        print('Extract Bucket:', bucket.name)
    for data_set in bucket.all_data_sets:
        data_set_to_json(data_set, results, verbose=verbose, require_hash_update=require_hash_update)


def data_set_to_json(data_set: DataSet, results: dict, verbose: bool = True, require_hash_update: bool = False):
    if verbose:
        print('Extract DataSet:', data_set.name)
    results[data_set.name] = dict()
    results[data_set.name]['properties'] = data_set.properties.data.to_dict()
    results[data_set.name]['metadata'] = data_set.metadata.data.to_dict()
    results[data_set.name]['children'] = dict()
    results[data_set.name]['data'] = dict()
    for child in data_set.all_data_sets:
        data_set_to_json(child,
                         results[data_set.name]['children'],
                         verbose=verbose,
                         require_hash_update=require_hash_update)
    for data in data_set.data:
        data_to_json(data, results[data_set.name]['data'], verbose=verbose, require_hash_update=require_hash_update)


def data_to_json(data: Data, results: dict, verbose: bool = True, require_hash_update: bool = False):
    if verbose:
        print('Extract Data:', data.name)
    results[data.name] = data.sha1(require_update=require_hash_update)


def recover_db(db_json: Union[dict, str],
               new_path: Union[str, Path],
               get_file: Union[None, Callable] = None,
               verbose: bool = True,
               overwrite: bool = False):
    if verbose:
        print('Recover Database to', new_path)
    if isinstance(db_json, str):
        db_json = loads(db_json)
    if isinstance(new_path, str):
        new_path = Path(new_path)
    if new_path.exists() and not overwrite:
        raise FileExistsError
    assert 'properties' in db_json
    properties = db_json['properties']
    assert 'name' in properties and 'buckets' in db_json
    name = properties['name']
    version = properties.get('version', 'alpha1')
    buckets = db_json['buckets']
    assert isinstance(buckets, dict)
    db = Database(name, path=str(new_path), version=version)
    recover_buckets(db, buckets, get_file=get_file, verbose=verbose)


def recover_buckets(db: Database,
                    buckets: dict,
                    get_file: Union[None, Callable] = None,
                    verbose: bool = True):
    for bucket_name, bucket_info in buckets.items():
        if verbose:
            print('Recover Bucket', bucket_name)
        properties = bucket_info['properties']
        metadata = bucket_info['metadata']
        children = bucket_info['children']
        assert isinstance(children, dict)
        new_bucket = Bucket(
            bucket_name=bucket_name,
            parent=db,
            metadata=metadata,
            properties=properties
        )
        new_bucket = db.insert_bucket(new_bucket)
        recover_data_sets(new_bucket, children, get_file=get_file, verbose=verbose)


def recover_data_sets(parent: Union[Bucket, DataSet],
                      data_sets: dict,
                      get_file: Union[None, Callable] = None,
                      verbose: bool = True):
    for data_set_name, data_set_info in data_sets.items():
        if verbose:
            print('Recover DataSet', data_set_name)
        properties = data_set_info['properties']
        metadata = data_set_info['metadata']
        children = data_set_info['children']
        data = data_set_info['data']
        new_data_set = DataSet(
            data_set_name=data_set_name,
            parent=parent,
            metadata=metadata,
            properties=properties
        )
        parent.insert_data_set(new_data_set)
        recover_data_sets(new_data_set, children, get_file=get_file, verbose=verbose)
        recover_data(new_data_set, data, get_file=get_file, verbose=verbose)


def recover_data(data_set: DataSet,
                 data: dict,
                 file: Union[None, str, Path] = None,
                 get_file: Union[None, Callable] = None,
                 verbose: bool = True):
    for data_name, data_sha1 in data.items():
        if verbose:
            print('Recover DataSet', data_name)
        new_data = data_set.add_data(data_name)
        if file:
            new_data.import_file(file, confirm=False)
        elif get_file is not None:
            new_data.import_file(get_file(data_sha1), confirm=False)
        else:
            print(f'WARNING: '
                  f'No file imported for data `{data_name}` with SHA1 value `{data_sha1}`. '
                  f'This may cause data loss.')


def get_data_list(db_json: dict) -> set:
    data_list = set()
    temp_dir = TemporaryDirectory()
    empty_file = Path(temp_dir.name) / 'empty.file'
    with open(empty_file, 'w') as fp:
        fp.write('')

    def list_objs(sha1: str) -> str:
        data_list.add(sha1)
        return str(empty_file)

    recover_db(db_json, temp_dir.name, list_objs, overwrite=True)
    temp_dir.cleanup()
    return data_list
