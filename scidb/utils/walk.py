from scidb.core import Database, Bucket, DataSet, Data
from typing import Union, Tuple, List
from pathlib import Path


NODE_TYPE = {
    'database': Database,
    'bucket': Bucket,
    'dataset': DataSet,
    'data': Data
}


class NodeWrapper:
    def __init__(self, node: Union[None, Database, Bucket, DataSet, Data]):
        self.__node__ = node

    @property
    def node(self):
        return self.__node__

    def __rshift__(self, child: Union[str, Tuple[type, str]]) -> 'NodeWrapper':
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

    def __lshift__(self, child: Union[str, Tuple[type, str]]) -> 'NodeWrapper':
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


def __convert_to_type__(type_or_name: Union[str, type]) -> type:
    if isinstance(type_or_name, type):
        return type
    elif isinstance(type_or_name, str):
        type_or_name = type_or_name.lower()
        if type_or_name in NODE_TYPE:
            return NODE_TYPE[type_or_name]
        else:
            raise KeyError
    else:
        raise TypeError


def walk_path(node: Union[Database, Bucket, DataSet, Data],
              path: List[Union[str, Tuple[Union[str, type], str], Tuple[Union[str, type], str, bool]]]) \
        -> Union[Bucket, DataSet, Data]:
    wrapper = walk(node)
    if len(path) == 0:
        return wrapper.node
    item = path[0]
    if isinstance(item, str):
        wrapper = wrapper << item
    elif isinstance(item, tuple):
        if len(item) == 2:
            node_type, node_name = item
            required = False
        elif len(item) == 3:
            node_type, node_name, required = item
        else:
            raise AssertionError
        node_type = __convert_to_type__(node_type)
        if required:
            wrapper = wrapper >> (node_type, node_name)
        else:
            wrapper = wrapper << (node_type, node_name)
    else:
        raise TypeError
    return walk_path(wrapper.node, path[1:])
