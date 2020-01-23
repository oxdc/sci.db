"""
Command list:

data create <name>
data list
data rm [-f] <name>
data import <name> <src_path>
data export <name> <dst_path>
"""

from scidb.core import DataSet, Data
from typing import List, Set
from pathlib import Path
import scidb.client.global_env as global_env


usage = """\
 1 | > data create <name>
   |   To create a data file.
 2 | > data list
   |   To list all data files in current directory.
 3 | > data rm [-f] <name>
   |   To delete a data file with given name.
 4 | > data import <name> <src_path>
   |   To import a data file.
 5 | > data export <name> <dst_path>
   |   To export a data file.
"""

create_usage = """\
> data create <name>

To create a data file.
<name> | REQUIRED | name of the new data file.
"""

list_usage = """\
> data list

To list all data files in current directory.
"""

rm_usage = """\
> data rm [-f] <name>

To delete a data file with given name.
-f     | OPTIONAL | force delete (without confirmation),
<name> | OPTIONAL | name of the data file.
"""

import_usage = """\
> data import [-f] <name> <src_path>

To import a data file.
-f         | OPTIONAL | force import (allow overwrite),
<name>     | REQUIRED | name of the data file,
<src_path> | REQUIRED | full path to the source file.
"""

export_usage = """\
> data export [-f] <name> <dst_path>

To export a data file.
-f         | OPTIONAL | force import (allow overwrite),
<name>     | REQUIRED | name of the data file,
<dst_path> | REQUIRED | full path to the destination file.
"""


def handler(args: List[str]):
    if len(args) < 1:
        print(usage)
        return
    if global_env.CURRENT_DATASET is None:
        print('No dataset selected.')
        return
    if not isinstance(global_env.CURRENT_DATASET, DataSet):
        print('Internal error.')
        exit(-1)
        return
    if args[0] == 'create':
        if len(args) != 2:
            print(create_usage)
            return
        create_data(args[1])
    elif args[0] == 'list':
        if len(args) != 1:
            print(list_usage)
            return
        list_data()
    elif args[0] == 'rm':
        if len(args) not in [2, 3]:
            print(rm_usage)
            return
        if '-f' in args:
            args.remove('-f')
            rm_data(args[1], confirm=True, feedback=True)
        else:
            rm_data(args[1], confirm=True,
                    feedback=input('Would you really want to PERMANENTLY delete this data? Y/[N]') == 'Y')
    elif args[0] == 'import':
        if len(args) not in [3, 4]:
            print(import_usage)
            return
        if '-f' in args:
            args.remove('-f')
            import_data(args[1], args[2], allow_overwrite=True)
        else:
            import_data(args[1], args[2])
    elif args[0] == 'export':
        if len(args) not in [3, 4]:
            print(export_usage)
            return
        if '-f' in args:
            args.remove('-f')
            export_data(args[1], args[2], allow_overwrite=True)
        else:
            export_data(args[1], args[2])
    else:
        print(usage)
        return


def get_current_dataset() -> DataSet:
    return global_env.CURRENT_DATASET


def create_data(name: str):
    global_env.CURRENT_DATASET.add_data(name)


def print_data(data: Set[Data]):
    print('  No. |    data name    ')
    print('------------------------')
    for i, d in enumerate(data):
        print(f'{i: 5} | {d.name: <15}')
    if len(data) == 0:
        print('       No records.      ')
    print('------------------------')


def list_data():
    print_data(global_env.CURRENT_DATASET.data)


def rm_data(name: str, confirm: bool = True, feedback: bool = False):
    if confirm and not feedback:
        print('User cancelled.')
        return
    global_env.CURRENT_DATASET.delete_data(name, confirm, feedback)


def import_data(name: str, src_path: [str, Path], allow_overwrite: bool = False):
    if global_env.CURRENT_DATASET.get_data(name) is not None and not allow_overwrite:
        print('Overwrite is not permitted. Try with `-f`.')
        return
    if global_env.CURRENT_DATASET.get_data(name) is None:
        global_env.CURRENT_DATASET.add_data(name)
    global_env.CURRENT_DATASET.get_data(name).import_file(src_path, allow_overwrite, confirm=True, feedback=True)


def export_data(name: str, dst_path: [str, Path], allow_overwrite: bool = False):
    if not isinstance(dst_path, Path):
        dst_path = Path(dst_path)
    if dst_path.exists() and not allow_overwrite:
        print('Overwrite is not permitted. Try with `-f`.')
        return
    data = global_env.CURRENT_DATASET.get_data(name)
    if data is not None:
        data.export_file(dst_path, allow_overwrite)
    else:
        print('No such data.')
