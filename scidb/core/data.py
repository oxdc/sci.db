import shutil
import hashlib
from pathlib import Path
from typing import TextIO, BinaryIO, IO, Union
from datetime import datetime
from os.path import getmtime
from .low import ObservableDict


class Data:
    def __init__(self, data_name: str, parent):
        self.__data_name__ = data_name
        self.__parent__ = parent
        self.init_metadata()
        self.init_properties()

    def init_metadata(self):
        if self.__data_name__ not in self.__parent__.metadata:
            self.__parent__.metadata[self.__data_name__] = dict()

    def init_properties(self):
        if self.__data_name__ not in self.__parent__.properties:
            self.__parent__.properties[self.__data_name__] = dict()

    def set_metadata(self, metadata: Union[None, dict], merge: bool = True):
        if metadata is None:
            return
        if merge:
            metadata = {**self.metadata, **metadata}
        self.__parent__.metadata[self.__data_name__] = metadata

    def set_properties(self, properties: Union[None, dict], merge: bool = True):
        if properties is None:
            return
        if merge:
            properties = {**self.properties, **properties}
        self.__parent__.properties[self.__data_name__] = properties

    @property
    def parent(self):
        return self.__parent__

    @property
    def path(self) -> Path:
        return self.__parent__.path / self.__data_name__

    @property
    def name(self) -> str:
        return self.__data_name__

    @property
    def metadata(self) -> ObservableDict:
        return self.__parent__.metadata[self.__data_name__]

    @property
    def properties(self) -> ObservableDict:
        return self.__parent__.properties[self.__data_name__]

    def rename(self, new_name: str):
        shutil.move(str(self.path), str(self.__parent__.path / new_name))
        self.__data_name__ = new_name

    def reader(self, binary: bool = False, **kwargs) -> [IO, BinaryIO, TextIO, None]:
        mode = 'r'
        mode += 'b' if binary else ''
        return open(str(self.path), mode=mode, **kwargs)

    def creator(self,
                binary: bool = False,
                confirm: bool = False,
                feedback: bool = False,
                **kwargs) -> [IO, BinaryIO, TextIO, None]:
        if confirm and not feedback:
            return None
        mode = 'x'
        mode += 'b' if binary else ''
        return open(str(self.path), mode=mode, **kwargs)

    def writer(self,
               binary: bool = False,
               append: bool = True,
               allow_overwrite: bool = False,
               confirm: bool = True,
               feedback: bool = False,
               **kwargs) -> [IO, BinaryIO, TextIO, None]:
        if not allow_overwrite and not append:
            raise PermissionError('Trying to overwrite existed data.')
        if confirm and not feedback:
            return
        mode = 'a' if append else 'w'
        mode += 'b' if binary else ''
        return open(str(self.path), mode=mode, **kwargs)

    def __repr__(self):
        return f"Data('{self.__data_name__}')"

    def import_file(self, src_path: [str, Path], allow_overwrite=False, confirm=True, feedback=False):
        if self.path.exists() and not allow_overwrite:
            return
        if confirm and not feedback:
            return
        shutil.copyfile(str(src_path), str(self.path))

    def export_file(self, dst_path: [str, Path], allow_overwrite=False):
        if Path(dst_path).exists() and not allow_overwrite:
            return
        shutil.copyfile(str(self.path), str(dst_path))

    def __calc_hash__(self, h, buffer_size: int = 131072):
        if not self.path.exists():
            return None
        with open(str(self.path), 'rb') as file_reader:
            while True:
                data = file_reader.read(buffer_size)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()

    def md5(self, buffer_size: int = 131072, require_update: bool = False) -> [str, None]:
        if not self.path.exists():
            return None
        last_modified_time = getmtime(str(self.path))
        if require_update \
                or 'md5' not in self.metadata \
                or 'md5-timestamp' not in self.metadata \
                or self.metadata['md5-timestamp'] < last_modified_time:
            result = self.__calc_hash__(hashlib.md5(), buffer_size)
            self.metadata['md5'] = result
            self.metadata['md5-timestamp'] = datetime.now().timestamp()
            return result
        else:
            return self.metadata['md5']

    def sha1(self, buffer_size: int = 131072, require_update: bool = False) -> [str, None]:
        if not self.path.exists():
            return None
        last_modified_time = getmtime(str(self.path))
        if require_update \
                or 'sha1' not in self.metadata \
                or 'sha1-timestamp' not in self.metadata \
                or self.metadata['sha1-timestamp'] < last_modified_time:
            result = self.__calc_hash__(hashlib.sha1(), buffer_size)
            self.metadata['sha1'] = result
            self.metadata['sha1-timestamp'] = datetime.now().timestamp()
            return result
        else:
            return self.metadata['sha1']

    def sha256(self, buffer_size: int = 131072, require_update: bool = False) -> [str, None]:
        if not self.path.exists():
            return None
        last_modified_time = getmtime(str(self.path))
        if require_update \
                or 'sha256' not in self.metadata \
                or 'sha256-timestamp' not in self.metadata \
                or self.metadata['sha256-timestamp'] < last_modified_time:
            result = self.__calc_hash__(hashlib.sha256(), buffer_size)
            self.metadata['sha256'] = result
            self.metadata['sha256-timestamp'] = datetime.now().timestamp()
            return result
        else:
            return self.metadata['sha256']
