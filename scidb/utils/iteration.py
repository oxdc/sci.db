from scidb.core import Bucket, DataSet
from typing import Callable


def iter_data_set(bucket_or_data_set: [Bucket, DataSet],
                  func: Callable,
                  include_deleted: bool = False,
                  depth: [None, int] = None,
                  **kwargs):
    if depth is not None and depth <= 0:
        return
    search_list = bucket_or_data_set.all_data_sets if include_deleted else bucket_or_data_set.data_sets
    for child in search_list:
        func(child, **kwargs)
        iter_data_set(child, func, include_deleted, depth=None if depth is None else depth - 1, **kwargs)


def iter_data(bucket_or_data_set: [Bucket, DataSet],
              func: Callable,
              include_deleted: bool = False,
              depth: [None, int] = None,
              **kwargs):
    if depth is not None and depth <= 0:
        return
    search_list = bucket_or_data_set.all_data_sets if include_deleted else bucket_or_data_set.data_sets
    for child in search_list:
        if isinstance(child, DataSet):
            for data in child.data:
                func(data, **kwargs)
        iter_data_set(child, func, include_deleted, depth=None if depth is None else depth - 1, **kwargs)
