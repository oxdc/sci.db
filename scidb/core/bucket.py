from .low.node import Node
from .data_set import DataSet
from .low.metadata import Metadata, Properties
from typing import Set


class Bucket(Node):
    def __init__(self,
                 bucket_name: str,
                 parent,
                 uuid: [None, str] = None,
                 deleted: [None, bool] = None,
                 metadata: [None, Metadata] = None,
                 properties: [None, Properties] = None):
        self.__data_sets__ = set()
        super().__init__(
            node_name=bucket_name,
            node_type='Bucket',
            parent=parent,
            uuid=uuid,
            deleted=deleted,
            metadata=metadata,
            properties=properties
        )
        self.init_data_sets()

    def init_data_sets(self):
        children = filter(lambda child: child.is_dir(), self.path.iterdir())
        for data_set in children:
            self.__data_sets__.add(DataSet(data_set.name, self))

    def add_data_set(self, name: str) -> DataSet:
        if self.get_data_set(name) is not None:
            raise FileExistsError
        new_data_set = DataSet(data_set_name=name, parent=self)
        self.__data_sets__.add(new_data_set)
        return new_data_set

    def get_data_set(self, name_or_uuid: str, include_deleted: bool = False) -> DataSet:
        target = None
        search_list = self.all_data_sets if include_deleted else self.data_sets
        for data_set in search_list:
            if data_set.name == name_or_uuid or data_set.uuid == name_or_uuid:
                target = data_set
        return target

    @property
    def data_sets(self) -> Set[DataSet]:
        return set(filter(lambda data_set: not data_set.deleted, self.__data_sets__))

    @property
    def trash(self) -> Set[DataSet]:
        return set(filter(lambda data_set: data_set.deleted, self.__data_sets__))

    @property
    def all_data_sets(self) -> Set[DataSet]:
        return self.__data_sets__

    def clear_trash(self, conform: bool = True, feedback: bool = False):
        if conform and not feedback:
            return
        for data_set in self.trash:
            data_set.purge_storage(conform, feedback)
        for data_set in self.data_sets:
            data_set.clear_trash(conform, feedback)
        self.__data_sets__ = set(self.data_sets)
