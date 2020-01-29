from scidb.core import Bucket, DataSet, Data
from typing import Union, List
from pathlib import Path


def get_by_path(bucket: Bucket, path: List[str]) -> Union[Bucket, DataSet, None]:
    target = bucket
    for segment in path:
        if target is None:
            return None
        else:
            target = target.get_data_set(segment, include_deleted=True)
    return target


def import_file(file: Union[str, Path],
                name: str,
                bucket: Bucket,
                path: List[str],
                metadata: Union[None, dict] = None,
                properties: Union[None, dict] = None,
                allow_overwrite: bool = True,
                confirm: bool = True,
                feedback: bool = False) -> Union[None, Data]:
    if confirm and not feedback:
        return None
    if isinstance(file, str):
        file = Path(file)
    if not file.exists():
        raise FileNotFoundError
    target = get_by_path(bucket, path)
    if target is None:
        raise FileNotFoundError
    if not allow_overwrite and target.get_data(name):
        return None
    if target.get_data(name):
        target.delete_data(name, confirm, feedback)
    data = target.add_data(name)
    data.set_metadata(metadata)
    data.set_properties(properties)
    return data


def import_dir(directory: Union[str, Path],
               bucket: Bucket,
               path: List[str],
               metadata: Union[None, dict] = None,
               properties: Union[None, dict] = None,
               allow_overwrite: bool = True,
               confirm: bool = True,
               feedback: bool = False) -> DataSet:
    pass


def import_tree(root: Union[str, Path],
                bucket_name: str,
                metadata: Union[None, dict] = None,
                properties: Union[None, dict] = None,
                allow_overwrite: bool = True,
                confirm: bool = True,
                feedback: bool = False) -> Bucket:
    pass
