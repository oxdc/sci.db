import shutil
from typing import TextIO, BinaryIO, IO


class Data:
    def __init__(self, data_name: str, parent):
        self.__data_name__ = data_name
        self.__parent__ = parent

    @property
    def path(self) -> str:
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
        shutil.move(self.path, self.__parent__.path / new_name)
        self.__data_name__ = new_name

    def reader(self, binary: bool = False, **kwargs) -> [IO, BinaryIO, TextIO, None]:
        mode = 'r'
        mode += 'b' if binary else ''
        return open(self.path, mode=mode, **kwargs)

    def creator(self,
                binary: bool = False,
                confirm: bool = False,
                feedback: bool = False,
                **kwargs) -> [IO, BinaryIO, TextIO, None]:
        if confirm and not feedback:
            return None
        mode = 'x'
        mode += 'b' if binary else ''
        return open(self.path, mode=mode, **kwargs)

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
        return open(self.path, mode=mode, **kwargs)
