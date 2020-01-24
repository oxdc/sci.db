"""
Command list:

dataset create <name>
dataset list [all | deleted]
dataset cd [<name> | <uuid> | .. | parent ]
dataset rm [<name> | <uuid>]
dataset restore [<name> | <uuid> | all]
dataset clean [-f] [<name> | <uuid> | all]
dataset tree [<name> | <uuid>]
dataset search [<name> | <uuid>]
"""

from scidb.core import Bucket, DataSet
from typing import List, Set, Union
import scidb.client.global_env as global_env


usage = """\
 1 | > dataset create <name>
   |   To create a dataset with given name in current directory.
 2 | > dataset list [all | deleted]
   |   To list (all / deleted) data sets of current directory.
 3 | > dataset cd [<name> | <uuid> | .. | parent ]
   |   To navigate into / out of a directory.
 4 | > dataset rm [<name> | <uuid>]
   |   To delete a dataset with given name or uuid.
 5 | > dataset restore [<name> | <uuid> | all]
   |   To restore a deleted dataset.
 6 | > dataset clean [-f] [<name> | <uuid> | all]
   |   To delete trash of a dataset / all data sets.
 7 | > dataset tree [<name> | <uuid> | . ]
   |   To print a tree of a given dataset or current directory.
 8 | > dataset search [<name> | <uuid>]
   |   To search a dataset within current bucket.
"""

create_usage = """\
> dataset create <name>

To create a dataset with given name in current directory.
<name> | REQUIRED | name of the new dataset.
"""

list_usage = """\
> dataset list [all | deleted]

To list (all / deleted) data sets of current directory.
all     | OPTIONAL | list all data sets.
deleted | OPTIONAL | list deleted data sets.
"""

cd_usage = """\
dataset cd [<name> | <uuid> | .. | parent ]

To navigate into / out of a directory.
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset,
..     | OPTIONAL | parent directory,
parent | OPTIONAL | parent directory.
"""

rm_usage = """\
> dataset rm [<name> | <uuid>]

To delete a dataset with given name or uuid.
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset.
"""

restore_usage = """\
> dataset restore [<name> | <uuid> | all]

To restore a deleted dataset.
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset,
all    | OPTIONAL | restore all data sets.
"""

clean_usage = """\
> dataset clean [-f] [<name> | <uuid> | all]

To delete trash of a dataset / all data sets.
-f     | OPTIONAL | force clean (without confirmation),
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset,
all    | OPTIONAL | clean all data sets.
"""

tree_usage = """\
> dataset tree [<name> | <uuid> | . ]

To print a tree of a given dataset or current directory.
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset,
.      | OPTIONAL | current directory.
"""

search_usage = """\
> dataset search [<name> | <uuid>]

To search a dataset within current bucket.
<name> | OPTIONAL | name of the dataset,
<uuid> | OPTIONAL | uuid of the dataset.
"""


def handler(args: List[str]):
    if len(args) < 1:
        print(usage)
        return
    if global_env.SELECTED_BUCKET is None:
        print('No bucket selected.')
        return
    if not isinstance(global_env.SELECTED_BUCKET, Bucket):
        print('Internal error.')
        exit(-1)
        return
    if args[0] == 'create':
        if len(args) != 2:
            print(create_usage)
            return
        create_data_set(args[1])
    elif args[0] == 'list':
        if len(args) == 1:
            list_data_set()
        elif len(args) == 2 and args[1] in ['all', 'deleted']:
            list_data_set(args[1])
        else:
            print(list_usage)
            return
    elif args[0] == 'cd':
        if len(args) != 2:
            print(cd_usage)
            return
        cd_data_set(args[1])
    elif args[0] == 'rm':
        if len(args) != 2:
            print(rm_usage)
            return
        rm_data_set(args[1])
    elif args[0] == 'restore':
        if len(args) != 2:
            print(restore_usage)
            return
        if 'all' in args:
            restore_data_set()
        else:
            restore_data_set(args[1])
    elif args[0] == 'clean':
        if len(args) not in [2, 3]:
            print(clean_usage)
            return
        if '-f' in args:
            args.remove('-f')
            if args[1] == 'all':
                clean_data_set(None, confirm=True, feedback=True)
            else:
                clean_data_set(args[1], confirm=True, feedback=True)
        else:
            if args[1] == 'all':
                clean_data_set(None, confirm=True,
                               feedback=input('Would you like to clean all data sets? Y/[N]') == 'Y')
            else:
                clean_data_set(args[1], confirm=True,
                               feedback=input('Would you like to clean this dataset? Y/[N]') == 'Y')
    elif args[0] == 'tree':
        if len(args) != 2:
            print(tree_usage)
            return
        print_tree_of_data_set(args[1])
    elif args[0] == 'search':
        if len(args) != 2:
            print(search_usage)
            return
        search_data_set(args[1])
    else:
        print(usage)
        return


def get_parent() -> Union[Bucket, DataSet]:
    return global_env.SELECTED_BUCKET if global_env.CURRENT_DATASET is None else global_env.CURRENT_DATASET


def create_data_set(data_set_name: str):
    get_parent().add_data_set(data_set_name)


def print_data_sets(data_sets: Set[DataSet]):
    print('  No. |  dataset name   |                 uuid                ')
    print('--------------------------------------------------------------')
    for i, data_set in enumerate(data_sets):
        print(f'{i: 5} | {data_set.name: <15} | {data_set.uuid}')
    if len(data_sets) == 0:
        print('                         No records.                          ')
    print('--------------------------------------------------------------')


def list_data_set(data_set_filter: Union[None, str] = None):
    if data_set_filter is None:
        print_data_sets(get_parent().data_sets)
    elif data_set_filter == 'all':
        print_data_sets(get_parent().all_data_sets)
    elif data_set_filter == 'deleted':
        print_data_sets(get_parent().trash)
    else:
        return


def cd_data_set(target: str):
    if target in ['..', 'parent']:
        if isinstance(get_parent(), Bucket):
            print('You cannot switch above buckets.')
        else:
            parent = get_parent().parent
            if isinstance(parent, DataSet):
                global_env.CURRENT_DATASET = parent
            else:
                global_env.CURRENT_DATASET = None
    else:
        data_set = get_parent().get_data_set(target)
        if data_set is not None:
            global_env.CURRENT_DATASET = data_set
        else:
            print('No such dataset.')


def rm_data_set(name_or_uuid: str):
    data_set = get_parent().get_data_set(name_or_uuid)
    if data_set is not None:
        data_set.delete()


def restore_data_set(name_or_uuid: Union[str, None] = None):
    if name_or_uuid is None:
        for deleted in get_parent().trash:
            deleted.restore()
    else:
        target = get_parent().get_data_set(name_or_uuid, include_deleted=True)
        if target is not None:
            target.restore()
        else:
            print('No such dataset.')


def clean_data_set(name_or_uuid: Union[str, None], confirm: bool = True, feedback: bool = False):
    if confirm and not feedback:
        print('User cancelled.')
        return
    if name_or_uuid is None:
        get_parent().clear_trash(confirm, feedback)
    else:
        data_set = get_parent().get_data_set(name_or_uuid)
        if data_set is None:
            print('No such dataset.')
        else:
            data_set.clear_trash(confirm, feedback)


def print_tree(data_set: DataSet, depth: int = 0):
    if depth > 0:
        prefix = ' |  ' * (depth - 1) + ' |--'
    else:
        prefix = ''
    print(f'{prefix} # {data_set.name} ({data_set.uuid})')
    for child in data_set.data_sets:
        print_tree(child, depth=depth + 1)


def print_tree_of_data_set(name_or_uuid: str):
    if name_or_uuid == '.':
        data_set = get_parent()
    else:
        data_set = get_parent().get_data_set(name_or_uuid)
    if data_set is None:
        print('No such dataset.')
    else:
        print_tree(data_set)


def print_search_results(target: Union[None, DataSet], path: List):
    if target is None:
        return
    print(f'@ Found result: {target.name} ({target.uuid})')
    print('  path:')
    path_str = [f'{item.name} ({item.uuid})' for item in path]
    path_str = ' --> \n'.join(path_str)
    print(path_str)


def search_data_set(name_or_uuid: str,
                    parent: Union[None, Bucket, DataSet] = None,
                    path: Union[None, List] = None):
    if path is None:
        path = []
    if parent is None:
        parent = global_env.SELECTED_BUCKET
    path.append(parent)
    target = parent.get_data_set(name_or_uuid)
    if target is not None:
        path.append(target)
        print_search_results(target, path)
    else:
        for child in parent.data_sets:
            search_data_set(name_or_uuid, child, path)
