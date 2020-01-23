from scidb.core import Bucket, DataSet, Data
from .iteration import iter_data_set, iter_data
from typing import Set, Callable


def search_data_set(bucket_or_data_set: [Bucket, DataSet], name_or_uuid: str, compare_func: [None, Callable] = None):
    def search_ds(ds: DataSet, r: Set):
        if compare_func is None:
            if ds.name == name_or_uuid or ds.uuid == name_or_uuid:
                r.add(ds)
        else:
            if compare_func(ds, name_or_uuid):
                r.add(ds)
    results = set()
    iter_data_set(bucket_or_data_set, search_ds, r=results)
    return results


def search_data(bucket_or_data_set: [Bucket, DataSet], name: str, compare_func: [None, Callable] = None):
    def search_d(d: Data, r: Set):
        if compare_func is None:
            if d.name == name:
                r.add(d)
        else:
            if compare_func(d, name):
                r.add(d)
    results = set()
    iter_data_set(bucket_or_data_set, search_d, r=results)
    return results
