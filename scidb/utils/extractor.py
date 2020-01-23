from scidb.core import Database, Bucket, DataSet, Data


def db_to_json(db_name: str, db_path: str):
    db = Database(db_name, db_path)
    results = dict()
    for bucket in db.all_buckets:
        results[bucket.name] = dict()
        results[bucket.name]['properties'] = bucket.properties.data.to_dict()
        results[bucket.name]['metadata'] = bucket.metadata.data.to_dict()
        results[bucket.name]['children'] = dict()
        bucket_to_json(bucket, results=results[bucket.name]['children'])
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
