from scidb.core import Database, Bucket, DataSet
from typing import Union

CONNECTED_DATABASE: Union[None, Database] = None
SELECTED_BUCKET: Union[None, Bucket] = None
CURRENT_DATASET: Union[None, DataSet] = None
