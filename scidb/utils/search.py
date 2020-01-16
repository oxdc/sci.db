from scidb.core import Bucket, DataSet
from .iteration import iter_data_set, iter_data
from typing import Set, Callable


def search_data_set(bucket_or_data_set: [Bucket, DataSet], name_or_uuid: str, compare_func: [None, Callable] = None):
    def search(ds: DataSet, r: Set):
        if compare_func is None:
            if ds.name == name_or_uuid or ds.uuid == name_or_uuid:
                r.add(ds)
        else:
            if compare_func(ds, name_or_uuid):
                r.add(ds)
    results = set()
    iter_data_set(bucket_or_data_set, search, r=results)
    return results
