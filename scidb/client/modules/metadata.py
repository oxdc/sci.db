"""
Command list:

property edit [<name> | <uuid>] <key_path> <value>
property delete [<name> | <uuid>] <key_path>
property show [<name> | <uuid>]
metadata edit [<name> | <uuid>] <key_path> <value>
metadata delete [<name> | <uuid>] <key_path>
metadata show [<name> | <uuid>]
"""

from scidb.core import Database, Bucket, DataSet
from typing import List, Any
import scidb.client.global_env as global_env


usage = """\
 1 | > property edit <node_type> [<name> | <uuid>] <key_path> <value>
   |   To create or edit an entry of property.
 2 | > property delete <node_type> [<name> | <uuid>] <key_path>
   |   To delete an entry of property.
 3 | > property show <node_type> [<name> | <uuid>]
   |   To show properties of an object.
 4 | > metadata edit <node_type> [<name> | <uuid>] <key_path> <value>
   |   To create or edit an entry of metadata.
 5 | > metadata delete <node_type> [<name> | <uuid>] <key_path>
   |   To delete an entry of metadata.
 6 | > metadata show <node_type> [<name> | <uuid>]
   |   To show metadata of an object.
"""

property_edit_usage = """\
> property edit <node_type> [<name> | <uuid>] <key_path> <value>

To create or edit an entry of property.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset,
<key_path>  | REQUIRED | path to a key,
<value>     | REQUIRED | property value.
"""

property_delete_usage = """\
> property delete <node_type> [<name> | <uuid>] <key_path>

To delete an entry of property.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset,
<key_path>  | REQUIRED | path to a key.
"""

property_show_usage = """\
> property show <node_type> [<name> | <uuid>]

To show properties of an object.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset.
"""

metadata_edit_usage = """\
> metadata edit <node_type> [<name> | <uuid>] <key_path> <value>

To create or edit an entry of metadata.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset,
<key_path>  | REQUIRED | path to a key,
<value>     | REQUIRED | property value.
"""

metadata_delete_usage = """\
> metadata delete <node_type> [<name> | <uuid>] <key_path>

To delete an entry of metadata.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset,
<key_path>  | REQUIRED | path to a key.
"""

metadata_show_usage = """\
> metadata show <node_type> [<name> | <uuid>]

To show metadata of an object.
<node_type> | REQUIRED | to specify a bucket / dataset or data,
<name>      | OPTIONAL | name of the dataset or data,
<uuid>      | OPTIONAL | uuid of the dataset.
"""


def handler(args: List[str]):
    if len(args) < 3:
        print(usage)
        return
    if global_env.SELECTED_BUCKET is None:
        print('No bucket selected.')
        return
    if not isinstance(global_env.SELECTED_BUCKET, Bucket):
        print('Internal error.')
        exit(-1)
        return
    if args[0] not in ['property', 'metadata']:
        print(usage)
        return
    if args[1] == 'edit':
        if len(args) < 6:
            print(metadata_edit_usage if args[0] == 'metadata' else property_edit_usage)
            return
        edit_entry(args[0], args[2], args[3], args[4], args[5])
    elif args[1] == 'delete':
        if len(args) < 5:
            print(metadata_delete_usage if args[0] == 'metadata' else property_delete_usage)
            return
        delete_entry(args[0], args[2], args[3], args[4])
    elif args[1] == 'show':
        if len(args) < 4:
            print(metadata_show_usage if args[0] == 'metadata' else property_show_usage)
            return
        show_entry(args[0], args[2], args[3])
    else:
        print(usage)
        return


def get_item(node_type: str, name_or_uuid: str) -> [Bucket, DataSet]:
    if node_type == 'bucket' and isinstance(global_env.CONNECTED_DATABASE, Database):
        return global_env.CONNECTED_DATABASE.get_bucket(name_or_uuid, include_deleted=True)
    elif node_type == 'dataset':
        parent = global_env.SELECTED_BUCKET if global_env.CURRENT_DATASET is None else global_env.CURRENT_DATASET
        if parent is not None:
            return parent.get_data_set(name_or_uuid, include_deleted=True)
        else:
            return None
    elif node_type == 'data':
        pass


def edit_entry(entry_type: str, node_type: str, name_or_uuid: str, key_path: str, value: Any):
    pass


def delete_entry(entry_type: str, node_type: str, name_or_uuid: str, key_path: str):
    pass


def show_entry(entry_type: str, node_type: str, name_or_uuid: str):
    pass
