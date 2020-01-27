from scidb.core import Database, Bucket, DataSet, Data
from typing import Union, Callable
from json import loads
from pathlib import Path


def db_to_json(db_name: str, db_path: str):
    db = Database(db_name, db_path)
    results = dict()
    results['properties'] = dict()
    results['properties']['name'] = db.name
    results['properties']['path'] = db.path
    results['properties']['version'] = db.version
    results['buckets'] = dict()
    for bucket in db.all_buckets:
        results['buckets'][bucket.name] = dict()
        results['buckets'][bucket.name]['properties'] = bucket.properties.data.to_dict()
        results['buckets'][bucket.name]['metadata'] = bucket.metadata.data.to_dict()
        results['buckets'][bucket.name]['children'] = dict()
        bucket_to_json(bucket, results=results['buckets'][bucket.name]['children'])
    return results


def bucket_to_json(bucket: Bucket, results: dict):
    for data_set in bucket.all_data_sets:
        data_set_to_json(data_set, results)


def data_set_to_json(data_set: DataSet, results: dict):
    results[data_set.name] = dict()
    results[data_set.name]['properties'] = data_set.properties.data.to_dict()
    results[data_set.name]['metadata'] = data_set.metadata.data.to_dict()
    results[data_set.name]['children'] = dict()
    results[data_set.name]['data'] = dict()
    for child in data_set.all_data_sets:
        data_set_to_json(child, results[data_set.name]['children'])
    for data in data_set.data:
        data_to_json(data, results[data_set.name]['data'])


def data_to_json(data: Data, results: dict):
    results[data.name] = data.sha1()


def recover_db(json: Union[dict, str], new_path: Union[str, Path]):
    if isinstance(json, str):
        json = loads(json)
    if isinstance(new_path, str):
        new_path = Path(new_path)
    if new_path.exists():
        raise FileExistsError
    assert 'name' in json and 'buckets' in json
    name = json['name']
    version = json.get('version', 'alpha1')
    buckets = json['buckets']
    assert isinstance(buckets, dict)
    db = Database(name, path=str(new_path), version=version)
    recover_buckets(db, buckets)


def recover_buckets(db: Database, buckets: dict):
    for bucket_name, bucket_info in buckets.items():
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
        recover_data_sets(new_bucket, children)


def recover_data_sets(parent: Union[Bucket, DataSet], data_sets: dict):
    for data_set_name, data_set_info in data_sets.items():
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
        recover_data_sets(new_data_set, children)
        recover_data(new_data_set, data)


def recover_data(
        data_set: DataSet,
        data: dict,
        file: Union[None, str, Path] = None,
        get_file: Union[None, Callable] = None):
    for data_name, data_sha1 in data.items():
        new_data = data_set.add_data(data_name)
        if file:
            new_data.import_file(file, confirm=False)
        elif get_file is not None:
            new_data.import_file(get_file(data_sha1), confirm=False)
        else:
            print(f'WARNING: '
                  f'No file imported for data `{data_name}` with SHA1 value `{data_sha1}`. '
                  f'This may cause data loss.')
