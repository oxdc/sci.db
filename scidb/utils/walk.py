from scidb.core import Database, Bucket, DataSet, Data
from typing import Union, Tuple
from pathlib import Path


class NodeWrapper:
    def __init__(self, node: Union[None, Database, Bucket, DataSet, Data]):
        self.__node__ = node

    @property
    def node(self):
        return self.__node__

    def __rshift__(self, child: Union[str, Tuple[Union[Bucket, DataSet, Data], str]]) -> 'NodeWrapper':
        node_identifier = ''
        node_type = None
        if isinstance(child, str):
            node_identifier = child
        elif isinstance(child, tuple):
            node_type, node_identifier = child
        if self.__node__ is None:
            raise LookupError
        elif isinstance(self.__node__, Database):
            if node_type in [None, Bucket]:
                return NodeWrapper(self.__node__.get_bucket(node_identifier, include_deleted=True))
            else:
                raise TypeError
        elif isinstance(self.__node__, Bucket):
            if node_type in [None, DataSet]:
                return NodeWrapper(self.__node__.get_data_set(node_identifier, include_deleted=True))
            else:
                raise TypeError
        elif isinstance(self.__node__, DataSet):
            if node_type in [None, DataSet, Data]:
                data_set = self.__node__.get_data_set(node_identifier, include_deleted=True)
                data = self.__node__.get_data(node_identifier)
                if data_set or node_type is DataSet:
                    return NodeWrapper(data_set)
                elif data or node_type is Data:
                    return NodeWrapper(data)
                else:
                    return NodeWrapper(None)
            else:
                raise TypeError
        elif isinstance(self.__node__, Data):
            raise LookupError
        else:
            raise TypeError

    def __lshift__(self, child: Union[str, Tuple[Union[Bucket, DataSet, Data], str]]) -> 'NodeWrapper':
        node_identifier = ''
        node_type = None
        if isinstance(child, str):
            node_identifier = child
        elif isinstance(child, tuple):
            node_type, node_identifier = child
        if self.__node__ is None:
            raise LookupError
        elif isinstance(self.__node__, Database):
            if node_type in [None, Bucket]:
                child_node = self.__node__.get_bucket(node_identifier, include_deleted=True)
                if child_node is not None:
                    return NodeWrapper(child_node)
                else:
                    return NodeWrapper(self.__node__.add_bucket(node_identifier))
            else:
                raise TypeError
        elif isinstance(self.__node__, Bucket):
            if node_type in [None, DataSet]:
                child_node = self.__node__.get_data_set(node_identifier, include_deleted=True)
                if child_node is not None:
                    return NodeWrapper(child_node)
                else:
                    return NodeWrapper(self.__node__.add_data_set(node_identifier))
            else:
                raise TypeError
        elif isinstance(self.__node__, DataSet):
            if node_type in [None, DataSet]:
                child_node = self.__node__.get_data_set(node_identifier, include_deleted=True)
                if child_node is not None:
                    return NodeWrapper(child_node)
                else:
                    return NodeWrapper(self.__node__.add_data_set(node_identifier))
            elif node_type is Data:
                child_node = self.__node__.get_data(node_identifier)
                if child_node is not None:
                    return NodeWrapper(child_node)
                else:
                    return NodeWrapper(self.__node__.add_data(node_identifier))
            else:
                raise TypeError
        elif isinstance(self.__node__, Data):
            raise LookupError
        else:
            raise TypeError

    def __add__(self, file: Union[str, Path]) -> Data:
        if isinstance(self.__node__, Data):
            if isinstance(file, str):
                file = Path(file)
            if file.exists():
                self.__node__.import_file(file, allow_overwrite=False, confirm=False)
            else:
                raise FileNotFoundError
            return self.__node__
        else:
            raise TypeError

    def __iadd__(self, file: Union[str, Path]) -> 'NodeWrapper':
        return NodeWrapper(self.__add__(file))


def walk(node: Union[Database, Bucket, DataSet, Data]):
    return NodeWrapper(node)
