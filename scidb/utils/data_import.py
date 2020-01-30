from scidb.core import Database, Bucket, DataSet, Data
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
                allow_overwrite: bool = False,
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
    data.import_file(file, allow_overwrite, confirm, feedback)
    data.set_metadata(metadata)
    data.set_properties(properties)
    return data


def import_dir(directory: Union[str, Path],
               name: str,
               bucket: Bucket,
               path: List[str],
               skip_dir: bool = False,
               metadata: Union[None, dict] = None,
               properties: Union[None, dict] = None,
               allow_overwrite: bool = False,
               confirm: bool = True,
               feedback: bool = False) -> Union[None, DataSet]:
    if confirm and not feedback:
        return None
    if isinstance(directory, str):
        directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError
    if not directory.is_dir():
        raise NotADirectoryError
    target = get_by_path(bucket, path)
    if target is None:
        raise FileNotFoundError
    if not target.get_data_set(name, include_deleted=True):
        target.add_data_set(name)
    data_set = target.get_data_set(name, include_deleted=True)
    inner_path = path.copy()
    inner_path.append(name)
    for child in directory.glob('*'):
        if child.is_file():
            if not allow_overwrite and data_set.get_data(child.name):
                return
            data_set.add_data(child.name).import_file(child, allow_overwrite, confirm, feedback)
        elif child.is_dir() and not skip_dir:
            import_dir(
                directory=child,
                name=child.name,
                bucket=bucket,
                path=inner_path,
                allow_overwrite=allow_overwrite,
                confirm=confirm,
                feedback=feedback
            )
    data_set.set_metadata(metadata)
    data_set.set_properties(properties)
    return data_set


def import_tree(root: Union[str, Path],
                bucket_name: str,
                db: Database,
                metadata: Union[None, dict] = None,
                properties: Union[None, dict] = None,
                allow_overwrite: bool = False,
                confirm: bool = True,
                feedback: bool = False) -> Union[None, Bucket]:
    if confirm and not feedback:
        return None
    if isinstance(root, str):
        root = Path(root)
    if not root.exists():
        raise FileNotFoundError
    if not root.is_dir():
        raise NotADirectoryError
    if not db.get_bucket(bucket_name, include_deleted=True):
        db.add_bucket(bucket_name)
    bucket = db.get_bucket(bucket_name, include_deleted=True)
    inner_path = []
    for child in root.glob('*'):
        if child.is_file():
            print(f'WARING: File {child.name} cannot be imported into bucket {bucket_name}.')
        elif child.is_dir():
            import_dir(
                directory=child,
                name=child.name,
                bucket=bucket,
                path=inner_path,
                allow_overwrite=allow_overwrite,
                confirm=confirm,
                feedback=feedback
            )
    bucket.set_metadata(metadata)
    bucket.set_properties(properties)
    return bucket
