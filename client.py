from scidb.client.modules.database import handler as db_handler
from scidb.client.modules.bucket import handler as bucket_handler
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
        if args[0] == 'db':
            db_handler(args[1:])
        elif args[0] == 'bucket':
            bucket_handler(args[1:])
        elif args[0] == 'exit':
            break
        else:
            print(usage)
