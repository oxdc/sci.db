"""
Command list:

db create <name> <path>
db connect <name> <path>
db close
db rm [-f] <name> <path>
"""

from scidb.core import Database
from typing import List, Union
from pathlib import Path
import scidb.client.global_env as global_env
import shutil


usage = """\
 1 | > db create <name> <path>
   |   To create a new database.
 2 | > db connect <name> <path>
   |   To connect an existing database.
 3 | > db close
   |   To close current connection.
 4 | > db rm [-f] <name> <path>
   |   To delete an existing database.
"""

create_usage = """\
> db create <name> <path>

To create a new database.
<name> | REQUIRED | name of the new database,
<path> | REQUIRED | absolute path to the root directory.
"""

connect_usage = """\
> db connect <name> <path>

To connect an existing database.
<name> | REQUIRED | name of the database,
<path> | REQUIRED | absolute path to the root directory.
"""

close_usage = """\
> db close

To close current connection.
"""

rm_usage = """\
db rm [-f] <name> <path>

To delete an existing database.
-f     | OPTIONAL | force delete (without confirmation), 
<name> | REQUIRED | name of the database,
<path> | REQUIRED | absolute path to the root directory.
"""


def handler(args: List[str]):
    if len(args) < 1:
        print(usage)
        return
    if args[0] == 'create':
        if len(args) != 3:
            print(create_usage)
            return
        create_db(args[1], args[2])
    elif args[0] == 'connect':
        if len(args) != 3:
            print(connect_usage)
            return
        connect_db(args[1], args[2])
    elif args[0] == 'close':
        if len(args) != 1:
            print(close_usage)
            return
        close_db()
    elif args[0] == 'rm':
        if len(args) not in [3, 4]:
            print(rm_usage)
            return
        if '-f' in args:
            args.remove('-f')
            rm_db(args[1], args[2], confirm=False)
        else:
            rm_db(args[1], args[2], confirm=True,
                  feedback=input('Do you really want to delete this database? Y/[N]') == 'Y')
    else:
        print(usage)
        return


def create_db(name: str, path: Union[str, Path]):
    try:
        global_env.CONNECTED_DATABASE = Database(name, path)
    except FileExistsError:
        print('The directory is not empty. Please specify an empty one.')


def connect_db(name: str, path: Union[str, Path]):
    global_env.CONNECTED_DATABASE = Database(name, path)


def close_db():
    global_env.CONNECTED_DATABASE = None


def rm_db(name: str, path: Union[str, Path], confirm: bool = True, feedback: bool = False):
    if confirm and not feedback:
        print('User cancelled.')
        return
    shutil.rmtree(path)
    print(f'{name} deleted.')
