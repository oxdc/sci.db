# sci.db
 
A simple scientific database written in pure Python.

## Install

```shell script
pip install oxdc-scidb
```

## Examples

```python
from scidb.core import Database

db = Database('my-scientific-database', '/path/to/your/db')

experiment_results = db.add_bucket('Experiment Results')
results_of_today = experiment_results.add_data_set('2020-01-01')
sample1 = results_of_today.add_data('sample_no1')
sample1.metadata['temperature'] = {'value': 300, 'unit': 'K'}
sample1.metadata['time'] = {'value': 120, 'unit': 'min'}
sample1.properties['element'] = 'H'
sample1.properties['series id'] = 'AS2001-K-09'

with sample1.writer(binary=False, append=True, allow_overwrite=False,
                    feedback=input('Would you like to record those data?').lower() == 'y') as writer:
    writer.write('some data ......')
```

## API

### Database

```python
from scidb.core import Database
db = Database('name', 'path')
db.add_bucket('name of bucket')
db.get_bucket('name of bucket')
db.touch_bucket('name of bucket')
db.insert_bucket(bucket)
db.rename('new name')
db.clear_trash(confirm=True, feedback=input('message').lower() == 'y')
for bucket in db.buckets:
    print(bucket.name)
```

### Bucket

```python
from scidb.core import Bucket
bucket = Bucket('name', db)
bucket.add_data_set('name of dataset')
bucket.get_data_set('name of dataset')
bucket.touch_data_set('name of dataset')
bucket.insert_data_set(data_set)
bucket.rename('new name')
bucket.delete()
bucket.restore()
bucket.clear_trash(confirm=True, feedback=input('message').lower() == 'y')
bucket.properties['key'] = 'value'
bucket.metadata['key'] = 'value'
bucket.copy_to(new_db)
bucket.move_to(new_db)
bucket.purge_storage(confirm=True, feedback=input('message').lower() == 'y')
bucket.is_type('Bucket')
for data_set in bucket.data_sets:
    print(data_set.name)
```

### Dataset

```python
from scidb.core import DataSet
data_set = DataSet('name', parent, bucket)
data_set.add_data_set('name of child')
data_set.get_data_set('name of child')
data_set.touch_data_set('name of child')
data_set.insert_data_set(child)
data_set.add_data('name of data')
data_set.get_data('name of data')
data_set.touch_data('name of data')
data_set.delete_data('name of data')
data_set.rename('new name')
data_set.delete()
data_set.restore()
data_set.clear_trash(confirm=True, feedback=input('message').lower() == 'y')
data_set.properties['key'] = 'value'
data_set.metadata['key'] = 'value'
data_set.copy_to(new_parent)
data_set.move_to(new_parent)
data_set.purge_storage(confirm=True, feedback=input('message').lower() == 'y')
data_set.is_type('DataSet')
for child in data_set.data_sets:
    print(child.name)
for data in data_set.data:
    print(data.name)
```

### Data

```python
from scidb.core import Data
data = Data('name', parent, bucket)
data.rename('new name')
data.writer(binary=True, append=False, allow_overwrite=True, confirm=True, feedback=input('message').lower() == 'y')
data.reader(binary=False, encoding='utf-8')
data.creator(binary=True, confirm=True, feedback=input('message').lower() == 'y')
data.properties['key'] = 'value'
data.metadata['key'] = 'value'
data.import_file('/path/to/file', allow_overwrite=True, confirm=True, feedback=input('message').lower() == 'y')
data.export_file('/path/to/file', allow_overwrite=True)
data.md5()
data.sha1()
data.sha256()
```

## Plugin: Backup backends

### Local backup

```python
from scidb.plugins.backup import LocalBackend
local_backup = LocalBackend('name of db', '/path/to/db', '/path/to/backup')
local_backup.connect()
local_backup.ping()
local_backup.create_backup()
local_backup.sync_backup()
local_backup.fetch_backup(time)
local_backup.list_backups()
local_backup.recover_from_backup(backup, '/path/to/restore')
local_backup.clean_objects(confirm=True, feedback=input('message').lower() == 'y')
```

### Minio backup

```python
from scidb.plugins.backup import MinioBackend
minio_backup = MinioBackend('name of db', '/path/to/db',
                            endpoint='localhost:3000', access_key='key', secret_key='secret',
                            secure=False, region='us')
minio_backup.connect()
minio_backup.ping()
minio_backup.create_backup()
minio_backup.sync_backup()
minio_backup.fetch_backup(time)
minio_backup.list_backups()
minio_backup.recover_from_backup(backup, '/path/to/restore')
minio_backup.clean_objects(confirm=True, feedback=input('message').lower() == 'y')
```

## Plugin: Sandbox

```python
from scidb.plugins.sandbox import SandboxManager
sandbox_manager = SandboxManager(db)
sandbox_manager.create_sandbox('name of sandbox')
sandbox_manager.get_sandbox('name of sandbox')
sandbox_manager.delete_sandbox('name of sandbox', confirm=True, feedback=input('message').lower() == 'y')
sandbox_manager.clear_sandbox('name of sandbox', confirm=True, feedback=input('message').lower() == 'y')
sandbox_manager.delete_all_sandboxes(confirm=True, feedback=input('message').lower() == 'y')
sandbox_manager.list_sandbox()
sandbox_manager.is_sandbox(bucket)
sandbox_manager.migrate_sandbox('name of sandbox',destination, delete_source=True, allow_overwrite=False,
                                confirm=True, feedback=input('message').lower() == 'y')
```

## Utils

### walk

```python
from scidb.utils.walk import walk, walk_path
anything_in_db = walk(any_node_in_db) >> 'required' << 'create if not existed' << (Data, 'type hint')
anything_in_db = walk_path(any_node_in_db, path=['path', 'to', 'something', 'else', ('data', 'type hint')])
```

### cleaner

```python
from scidb.utils.cleaner import clean_useless_metadata, clean_useless_properties
clean_useless_properties(data_set, judge=function, confirm=True, feedback=input('message').lower() == 'y')
clean_useless_metadata(data_set, judge=function, confirm=True, feedback=input('message').lower() == 'y')
```

### search

```python
from scidb.utils.search import search_data, search_data_set
search_data(bucket_or_data_set, compare_func=function)
search_data_set(bucket_or_data_set, 'name or uuid', compare_func=function)
```

### iteration

```python
from scidb.utils.iteration import iter_data, iter_data_set
iter_data(bucket_or_data_set, function, include_deleted=False, depth=10)
iter_data_set(bucket_or_data_set, function, include_deleted=False, depth=10)
```

### migrator

```python
from scidb.utils.migrator import migrate
migrate(source, destination, delete_source=True, allow_overwrite=False,
        confirm=True, feedback=input('message').lower() == 'y')
```

### data_import

```python
from scidb.utils.data_import import import_file, import_dir, import_tree
import_dir('/path/to/dir', 'name of dataset', bucket, path=['path', 'to', 'destination'], skip_dir=False,
           metadata={'key': 'value'}, properties={'key': 'value'},
           allow_overwrite=False, confirm=True, feedback=input('message').lower() == 'y')
import_file('/path/to/file', 'data name', bucket, path=['path', 'to', 'dataset'],
            metadata={'key': 'value'}, properties={'key': 'value'},
            allow_overwrite=False, confirm=True, feedback=input('message').lower() == 'y')
import_tree('/path/to/root', 'bucket name', db,
            metadata={'key': 'value'}, properties={'key': 'value'},
            allow_overwrite=False, confirm=True, feedback=input('message').lower() == 'y')
```

### extractor

```python
from scidb.utils.extractor import db_to_json, recover_db
db_to_json('db name', '/path/to/db')
recover_db(db_json, '/path/to/recover')
```
