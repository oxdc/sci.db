"""
Command list:

dataset create <name>
dataset list [all | deleted]
dataset cd [<name> | <uuid> | .. | parent ]
dataset rm [<name> | <uuid>]
dataset clean [-f] [<name> | <uuid> | all]
dataset tree [<name> | <uuid>]
dataset search [<name> | <uuid>]
"""

from scidb.core import Database, Bucket, DataSet
from typing import List, Set
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
 5 | > dataset clean [-f] [<name> | <uuid> | all]
   |   To delete trash of a dataset / all data sets.
 6 | > dataset tree [<name> | <uuid> | . ]
   |   To print a tree of a given dataset or current directory.
 7 | > dataset search [<name> | <uuid>]
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


def get_parent() -> [DataSet, Bucket]:
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


def list_data_set(data_set_filter: [None, str] = None):
    if data_set_filter is None:
        print_data_sets(get_parent().data_sets)
    elif data_set_filter == 'all':
        print_data_sets(get_parent().all_data_sets)
    elif data_set_filter == 'deleted':
        print_data_sets(get_parent().trash)
    else:
        return
