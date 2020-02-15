from scidb.core import DataSet, Metadata, Properties
from typing import Union, List, Callable, Any


def empty_dicts(key: str, value: Any, current_path: List[str]) -> bool:
    if isinstance(value, dict) or isinstance(value, Metadata) or isinstance(value, Properties):
        return bool(value)


def iter_node_dict(node_dict: Union[Metadata, Properties, dict],
                   path: List[str],
                   func: Callable = empty_dicts,
                   confirm: bool = True,
                   feedback: bool = False,
                   **kwargs):
    for key, value in node_dict.items():
        current_path = path + [key]
        if func(key, value, current_path, **kwargs):
            if confirm and not feedback:
                continue
            del key
        if isinstance(value, dict) or isinstance(value, Metadata) or isinstance(value, Properties):
            iter_node_dict(
                node_dict=value,
                path=current_path,
                func=func,
                confirm=confirm,
                feedback=feedback,
                **kwargs
            )


def clean_useless_properties(data_set: DataSet,
                             func: Callable = empty_dicts,
                             confirm: bool = True,
                             feedback: bool = False):
    iter_node_dict(data_set.properties, [str(data_set.path), 'properties'], func, confirm, feedback)


def clean_useless_metadata(data_set: DataSet,
                           func: Callable = empty_dicts,
                           confirm: bool = True,
                           feedback: bool = False):
    iter_node_dict(data_set.metadata, [str(data_set.path), 'metadata'], func, confirm, feedback)
