from .low.node import Node
from .data import Data
from .low.metadata import Metadata, Properties
from uuid import UUID
from typing import Set, List, Union


class DataSet(Node):
    def __init__(self,
                 data_set_name: str,
                 parent,
                 uuid: Union[None, str, UUID] = None,
                 deleted: Union[None, bool] = None,
                 metadata: Union[None, Metadata] = None,
                 properties: Union[None, Properties] = None):
        self.__data_sets__ = set()
        self.__data__ = set()
        super().__init__(
            node_name=data_set_name,
            node_type='DataSet',
            parent=parent,
            uuid=uuid,
            deleted=deleted,
            metadata=metadata,
            properties=properties
        )
        self.init_data_sets()
        self.init_data()

    def init_data_sets(self):
        children = filter(lambda child: child.is_dir(), self.path.iterdir())
        for data_set in children:
            self.__data_sets__.add(DataSet(data_set.name, self))

    def init_data(self):
        children = filter(lambda child: child.is_file() and child.name not in self.RESERVED_NAMES, self.path.iterdir())
        for data in children:
            self.__data__.add(Data(data.name, self))

    def add_data_set(self, name: str) -> 'DataSet':
        if self.get_data_set(name, include_deleted=True) is not None:
            raise FileExistsError
        new_data_set = DataSet(data_set_name=name, parent=self)
        self.__data_sets__.add(new_data_set)
        return new_data_set

    def insert_data_set(self, data_set: 'DataSet') -> 'DataSet':
        if data_set in self.__data_sets__ \
                or self.get_data_set(data_set.name, include_deleted=True) is not None \
                or self.get_data_set(data_set.uuid, include_deleted=True) is not None:
            raise FileExistsError
        self.__data_sets__.add(data_set)
        return data_set

    def get_data_set(self, name_or_uuid: str, include_deleted: bool = False) -> 'DataSet':
        target = None
        search_list = self.all_data_sets if include_deleted else self.data_sets
        for data_set in search_list:
            if data_set.name == name_or_uuid or data_set.uuid == name_or_uuid:
                target = data_set
        return target

    def add_data(self, name: str) -> Data:
        if self.get_data(name) is not None:
            raise FileExistsError
        new_data = Data(data_name=name, parent=self)
        self.__data__.add(new_data)
        return new_data

    def get_data(self, name: str) -> Data:
        target = None
        for data in self.__data__:
            if data.name == name:
                target = data
        return target

    def delete_data(self, name: str, confirm: bool = True, feedback: bool = False):
        if confirm and not feedback:
            return
        target = None
        for data in self.__data__:
            if data.name == name:
                target = data
        if target is None:
            return
        if target.path.exists():
            target.path.unlink()
        self.__data__.remove(target)

    def clear_trash(self, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        for data_set in self.trash:
            data_set.purge_storage(conform, feedback)
        for data_set in self.data_sets:
            data_set.clear_trash(conform, feedback)
        self.__data_sets__ = set(self.data_sets)

    @property
    def data_sets(self) -> Set['DataSet']:
        return set(filter(lambda data_set: not data_set.deleted, self.__data_sets__))

    @property
    def trash(self) -> Set['DataSet']:
        return set(filter(lambda data_set: data_set.deleted, self.__data_sets__))

    @property
    def all_data_sets(self) -> Set['DataSet']:
        return self.__data_sets__

    @property
    def data(self) -> Set[Data]:
        return self.__data__

    @property
    def RESERVED_NAMES(self) -> List[str]:
        return [
            'metadata.json',
            'properties.json',
            'metadata.yml',
            'properties.yml',
            'metadata.yaml',
            'properties.yaml'
        ]
