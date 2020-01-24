from scidb.client.modules import db_handler, bucket_handler, data_set_handler, data_handler, metadata_handler
import shlex


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
        args = shlex.split(commands)
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
        elif args[0] in ['p', 'property', 'm', 'metadata']:
            metadata_handler(args)
        elif args[0] == 'exit':
            break
        else:
            print(usage)
