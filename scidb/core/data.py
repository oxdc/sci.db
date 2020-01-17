import shutil
from pathlib import Path
from typing import TextIO, BinaryIO, IO


class Data:
    def __init__(self, data_name: str, parent):
        self.__data_name__ = data_name
        self.__parent__ = parent
        self.init_metadata()

    def init_metadata(self):
        self.__parent__.metadata[self.__data_name__] = dict()

    @property
    def path(self) -> Path:
        return self.__parent__.path / self.__data_name__

    @property
    def name(self) -> str:
        return self.__data_name__

    @property
    def metadata(self):
        if self.__data_name__ in self.__parent__.metadata:
            return self.__parent__.metadata[self.__data_name__]
        else:
            return None

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
