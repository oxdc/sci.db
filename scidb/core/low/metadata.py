import json
import yaml
import enum


class MetadataFileType(enum.Enum):
    Json = 0
    Yaml = 1


class ObservableDict(dict):
    def __init__(self, init_dict, callback):
        self.__callback__ = callback
        for item, value in init_dict.items():
            if isinstance(value, dict):
                init_dict[item] = ObservableDict(value, callback)
        super().__init__(init_dict)

    def __setitem__(self, item, value):
        value = ObservableDict(value, self.__callback__) if isinstance(value, dict) else value
        super().__setitem__(item, value)
        self.__callback__(item, value)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def set(self, item, value):
        self.__setitem__(item, value)

    def get(self, item):
        return self.__getitem__(item)

    def to_dict(self):
        return {key: dict(value) if isinstance(value, ObservableDict) else value for key, value in self.items()}


class NodeDict:
    def __init__(self, node, filename, data=None, file_type=MetadataFileType.Json):
        self.__node__ = node
        self.__filename__ = filename
        self.__data__ = ObservableDict({}, self.callback)
        self.__file_type__ = file_type
        if data is not None:
            self.__data__ = data if isinstance(data, ObservableDict) else ObservableDict(data, self.callback)
            self.callback()
        else:
            self.__load_data__()

    def __load_data__(self):
        full_path = self.__node__.path / self.__filename__
        if not full_path.exists() or not full_path.is_file():
            return
        else:
            full_path = str(full_path)
            with open(full_path) as file:
                if self.__file_type__ == MetadataFileType.Yaml:
                    self.__data__ = ObservableDict(yaml.load(file), self.callback)
                elif self.__file_type__ == MetadataFileType.Json:
                    self.__data__ = ObservableDict(json.load(file), self.callback)
                else:
                    raise NotImplementedError('The type is not supported.')

    def callback(self, item=None, value=None):
        full_path = str(self.__node__.path / self.__filename__)
        with open(full_path, mode='w') as file:
            if self.__file_type__ == MetadataFileType.Yaml:
                yaml.dump(self.__data__.to_dict(), file)
            elif self.__file_type__ == MetadataFileType.Json:
                json.dump(self.__data__, file)
            else:
                raise NotImplementedError('The type is not supported.')

    def __setitem__(self, item, value):
        self.__data__[item] = value

    def __getitem__(self, item):
        return self.__data__[item]

    def __contains__(self, item):
        return item in self.__data__

    def set(self, item, value):
        self.__setitem__(item, value)

    def get(self, item):
        return self.__getitem__(item)

    @property
    def data(self):
        return self.__data__


class Metadata(NodeDict):
    def __init__(self, node, data=None, file_type=MetadataFileType.Json):
        if file_type == MetadataFileType.Yaml:
            filename = 'metadata.yml'
        elif file_type == MetadataFileType.Json:
            filename = 'metadata.json'
        else:
            raise NotImplementedError('The type is not supported.')
        super().__init__(node, data=data, filename=filename, file_type=file_type)


class Properties(NodeDict):
    def __init__(self, node, data=None, file_type=MetadataFileType.Json):
        if file_type == MetadataFileType.Yaml:
            filename = 'properties.yml'
        elif file_type == MetadataFileType.Json:
            filename = 'properties.json'
        else:
            raise NotImplementedError('The type is not supported.')
        super().__init__(node, data=data, filename=filename, file_type=file_type)