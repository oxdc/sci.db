"""
Command list:

bucket select [<name> | <uuid>]
bucket create <name>
bucket list [all | deleted]
bucket rm [<name> | <uuid>]
bucket restore [<name> | <uuid> | all]
bucket clean [-f] [<name> | <uuid> | all]
"""

from scidb.core import Database, Bucket
from typing import List, Set
import scidb.client.global_env as global_env


usage = """\
 1 | > bucket select [<name> | <uuid>]
   |   To select a bucket.
 2 | > bucket create <name>
   |   To create a bucket with given name.
 3 | > bucket list [all | deleted]
   |   To list (all / deleted) buckets.
 4 | > bucket rm [<name> | <uuid>]
   |   To delete a bucket with given name or uuid.
 5 | > bucket restore [<name> | <uuid> | all]
   |   To restore a deleted bucket.
 6 | > bucket clean [-f] [<name> | <uuid> | all]
   |   To delete trash of a bucket / all buckets.
"""

select_usage = """\
> bucket select [<name> | <uuid>]

To select a bucket.
<name> | OPTIONAL | name of the bucket,
<uuid> | OPTIONAL | uuid of the bucket.
"""

create_usage = """\
> bucket create <name>

To create a bucket with given name.
<name> | REQUIRED | name of the new bucket.
"""

list_usage = """\
> bucket list [all | deleted]

To list (all / deleted) buckets.
all     | OPTIONAL | list all buckets.
deleted | OPTIONAL | list deleted buckets.
"""

rm_usage = """\
> bucket rm [<name> | <uuid>]

To delete a bucket with given name or uuid.
<name> | OPTIONAL | name of the bucket,
<uuid> | OPTIONAL | uuid of the bucket.
"""

restore_usage = """\
> bucket restore [<name> | <uuid> | all]

To restore a deleted bucket.
<name> | OPTIONAL | name of the bucket,
<uuid> | OPTIONAL | uuid of the bucket,
all    | OPTIONAL | restore all buckets.
"""

clean_usage = """\
> bucket clean [-f] [<name> | <uuid> | all]

To delete trash of a bucket / all buckets.
-f     | OPTIONAL | force clean (without confirmation),
<name> | OPTIONAL | name of the bucket,
<uuid> | OPTIONAL | uuid of the bucket,
all    | OPTIONAL | clean all buckets.
"""


def handler(args: List[str]):
    if len(args) < 1:
        print(usage)
        return
    if global_env.CONNECTED_DATABASE is None:
        print('No database connected.')
        return
    if not isinstance(global_env.CONNECTED_DATABASE, Database):
        print('Internal error.')
        exit(-1)
        return
    if args[0] == 'select':
        if len(args) != 2:
            print(select_usage)
            return
        select_bucket(args[1])
    elif args[0] == 'create':
        if len(args) != 2:
            print(create_usage)
            return
        create_bucket(args[1])
    elif args[0] == 'list':
        if len(args) == 1:
            list_bucket()
        elif len(args) == 2 and args[1] in ['all', 'deleted']:
            list_bucket(args[1])
        else:
            print(list_usage)
            return
    elif args[0] == 'rm':
        if len(args) != 2:
            print(rm_usage)
            return
        rm_bucket(args[1])
    elif args[0] == 'restore':
        if len(args) != 2:
            print(restore_usage)
            return
        if 'all' in args:
            restore_bucket()
        else:
            restore_bucket(args[1])
    elif args[0] == 'clean':
        if len(args) not in [2, 3]:
            print(clean_usage)
            return
        if '-f' in args:
            args.remove('-f')
            if args[1] == 'all':
                clean_bucket(None, confirm=True, feedback=True)
            else:
                clean_bucket(args[1], confirm=True, feedback=True)
        else:
            if args[1] == 'all':
                clean_bucket(None, confirm=True,
                             feedback=input('Would you like to clean all buckets? Y/[N]') == 'Y')
            else:
                clean_bucket(args[1], confirm=True,
                             feedback=input('Would you like to clean this bucket? Y/[N]') == 'Y')
    else:
        print(usage)
        return


def select_bucket(name_or_uuid: str):
    global_env.SELECTED_BUCKET = global_env.CONNECTED_DATABASE.get_bucket(name_or_uuid, include_deleted=True)


def create_bucket(bucket_name: str):
    global_env.SELECTED_BUCKET = global_env.CONNECTED_DATABASE.add_bucket(bucket_name)


def print_buckets(buckets: Set[Bucket]):
    print('  No. |   bucket name   |                 uuid                ')
    print('--------------------------------------------------------------')
    for i, bucket in enumerate(buckets):
        print(f'{i: 5} | {bucket.name: <15} | {bucket.uuid}')
    if len(buckets) == 0:
        print('                         No records.                          ')
    print('--------------------------------------------------------------')


def list_bucket(bucket_filter: [None, str] = None):
    if bucket_filter is None:
        print_buckets(global_env.CONNECTED_DATABASE.buckets)
    elif bucket_filter == 'all':
        print_buckets(global_env.CONNECTED_DATABASE.all_buckets)
    elif bucket_filter == 'deleted':
        print_buckets(global_env.CONNECTED_DATABASE.trash)
    else:
        return


def rm_bucket(name_or_uuid: str):
    bucket = global_env.CONNECTED_DATABASE.get_bucket(name_or_uuid)
    if bucket is not None:
        bucket.delete()


def restore_bucket(name_or_uuid: [str, None] = None):
    if name_or_uuid is None:
        for deleted in global_env.CONNECTED_DATABASE.trash:
            deleted.restore()
    else:
        target = global_env.CONNECTED_DATABASE.get_bucket(name_or_uuid, include_deleted=True)
        if target is not None:
            target.restore()
        else:
            print('No such bucket.')


def clean_bucket(name_or_uuid: [str, None], confirm: bool = True, feedback: bool = False):
    if confirm and not feedback:
        print('User cancelled.')
        return
    if name_or_uuid is None:
        global_env.CONNECTED_DATABASE.clear_trash(confirm, feedback)
    else:
        bucket = global_env.CONNECTED_DATABASE.get_bucket(name_or_uuid)
        if bucket is None:
            print('No such bucket.')
        else:
            bucket.clear_trash(confirm, feedback)
