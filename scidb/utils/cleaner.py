from scidb.core import DataSet, Metadata, Properties
from typing import Union, List, Callable, Any


def empty_dicts(key: str, value: Any, current_path: List[str]) -> bool:
    if isinstance(value, dict) or isinstance(value, Metadata) or isinstance(value, Properties):
        return bool(value)


def iter_node_dict(node_dict: Union[Metadata, Properties, dict],
                   path: List[str],
                   judge: Callable = empty_dicts,
                   confirm: bool = True,
                   feedback: bool = False,
                   **kwargs):
    keys_to_delete = []
    for key, value in node_dict.items():
        current_path = path + [key]
        if judge(key, value, current_path, **kwargs):
            if confirm and not feedback:
                continue
            keys_to_delete.append(key)
        if isinstance(value, dict) or isinstance(value, Metadata) or isinstance(value, Properties):
            iter_node_dict(
                node_dict=value,
                path=current_path,
                judge=judge,
                confirm=confirm,
                feedback=feedback,
                **kwargs
            )
    for key in keys_to_delete:
        node_dict.pop(key)


def clean_useless_properties(data_set: DataSet,
                             judge: Callable = empty_dicts,
                             confirm: bool = True,
                             feedback: bool = False):
    iter_node_dict(data_set.properties, [str(data_set.path), 'properties'], judge, confirm, feedback)


def clean_useless_metadata(data_set: DataSet,
                           judge: Callable = empty_dicts,
                           confirm: bool = True,
                           feedback: bool = False):
    iter_node_dict(data_set.metadata, [str(data_set.path), 'metadata'], judge, confirm, feedback)
