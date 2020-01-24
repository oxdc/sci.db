import shutil
from pathlib import Path
from uuid import uuid4, UUID
from typing import Union
from .metadata import Metadata, Properties


class Node:
    def __init__(self,
                 node_name: str,
                 node_type: str,
                 parent=None,
                 uuid: Union[None, UUID] = None,
                 deleted: Union[None, bool] = None,
                 metadata: Union[None, Metadata] = None,
                 properties: Union[None, Properties] = None):
        self.__node_name__ = node_name
        self.__node_type__ = node_type
        self.__parent__ = parent
        self.__uuid__ = str(uuid) if uuid else str(uuid4())
        self.init_storage()
        self.metadata = metadata if metadata and isinstance(metadata, Metadata) else Metadata(self)
        self.properties = properties if properties and isinstance(properties, Properties) else Properties(self)
        self.properties['node_name'] = node_name
        self.properties['node_type'] = node_type
        if 'uuid' in self.properties:
            self.__uuid__ = self.properties['uuid']
        else:
            self.properties['uuid'] = self.__uuid__
        if deleted is not None:
            self.properties['deleted'] = deleted
        elif 'deleted' not in self.properties:
            self.properties['deleted'] = False

    @property
    def uuid(self) -> str:
        return self.__uuid__

    @property
    def name(self) -> str:
        return self.__node_name__

    def rename(self, new_name: str):
        self.move_storage(self.__parent__.path / new_name)
        self.__node_name__ = new_name
        self.properties['node_name'] = new_name

    @property
    def node_type(self) -> str:
        return self.__node_type__

    def is_type(self, type_to_compare: str) -> bool:
        return self.__node_type__ == type_to_compare

    @property
    def parent(self):
        return self.__parent__

    def change_parent(self, new_parent):
        self.move_storage(new_parent.path / self.name)
        self.__parent__ = new_parent

    @property
    def path(self) -> Path:
        return self.__parent__.path / self.__node_name__

    @property
    def node_id(self) -> str:
        return self.__uuid__

    @property
    def deleted(self) -> bool:
        return self.properties['deleted']

    def delete(self):
        self.properties['deleted'] = True

    def restore(self):
        self.properties['deleted'] = False

    def init_storage(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def move_storage(self, new_path: Union[str, Path]):
        shutil.move(str(self.path), str(new_path))

    def purge_storage(self, confirm: bool = True, feedback: bool = False):
        if confirm and not feedback:
            return
        self.delete()
        shutil.rmtree(str(self.path))

    def __eq__(self, other: 'Node'):
        return self.__uuid__ == other.uuid

    def __hash__(self) -> int:
        return UUID(self.__uuid__).int

    def __repr__(self) -> str:
        return f"{self.__node_type__}('{self.__node_name__}')"


class Root:
    def __init__(self, root_path: Union[str, Path]):
        if isinstance(root_path, Path):
            self.__root_path__ = root_path
        else:
            self.__root_path__ = Path(root_path)

    @property
    def path(self) -> Path:
        return self.__root_path__

    def init_storage(self):
        self.__root_path__.mkdir(parents=True)
