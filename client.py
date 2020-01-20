from scidb.client.modules.database import handler as db_handler
from scidb.client.modules.bucket import handler as bucket_handler
from scidb.client.modules.data_set import handler as data_set_handler
from scidb.client.modules.data import handler as data_handler
import re


usage = """\
 1 | > db ...
   |   To manipulate databases.
 2 | > bucket ...
   |   To manipulate buckets.
 3 | > dataset ...
   |   To manipulate data sets.
 4 | > data ...
   |   To manipulate data.
"""


if __name__ == "__main__":
    print('Sci.DB client.')
    while True:
        commands = input('> ')
        args = re.split(r'\s+', commands)
        if len(args) < 1:
            print(usage)
        if args[0] in ['db', 'database']:
            db_handler(args[1:])
        elif args[0] in ['bk', 'bucket']:
            bucket_handler(args[1:])
        elif args[0] in ['ds', 'dataset']:
            data_set_handler(args[1:])
        elif args[0] in ['d', 'data']:
            data_handler(args[1:])
        elif args[0] == 'exit':
            break
        else:
            print(usage)
