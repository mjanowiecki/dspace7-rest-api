import pandas as pd
import argparse
import secret
import time
import mimetypes
import json
from datetime import datetime
from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Item, Bundle
from authenticateToDSpace import authenticate_to_dspace
import os
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
from tqdm import tqdm
from http.client import HTTPConnection

HTTPConnection.__init__.__defaults__ = tuple(
    x if x != 8192 else 64*1024
    for x in HTTPConnection.__init__.__defaults__
)

startTime = time.time()

secretsVersion = input('To edit production server, enter secrets file: ')
if secretsVersion != '':
    try:
        secret = __import__(secretsVersion)
        print('Editing Production')
    except ImportError:
        print('Editing Stage')
else:
    print('Editing Stage')

base_url = secret.base_url
user = secret.user
password = secret.password

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

file_directory = '/Users/michelle/Desktop/science_review'
parent = '1bddfd96-c851-445a-89ce-caaa433dbdf3'
# Authenticate to DSpace site, get token
d = DSpaceClient(api_endpoint=base_url, username=user, password=password, fake_user_agent=False)

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print(f'Error logging in! Giving up.')
    exit(1)

fields_with_language = ['dc.description.abstract', 'dc.description.provenance', 'dc.title',
                        'dc.title.alternative', 'dc.relation.ispartofseries', 'dc.rights',
                        'dc.type', 'dc.subject', 'dc.relation']


def create_bitstream(uuid_bundle, f_name, mimetype, file_location):
    file_stats = os.stat(file_location)
    file_size = file_stats.st_size
    session = authenticate_to_dspace(base_url, user, password)
    headers = session.headers
    bitstream_url = f'{base_url}/core/bundles/{uuid_bundle}/bitstreams'
    # Create multipart form data
    properties = {'name': f_name,
                  'metadata': {'dc.description': [{'value': 'Bitstream uploaded by Python REST Client',
                                                   'language': 'en',
                                                   'authority': None,
                                                   'confidence': -1,
                                                   'place': 0}]},
                  'bundleName': 'ORIGINAL'}
    with open(file_location, 'rb') as f:
        with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as bar:
            e = MultipartEncoder({'properties': json.dumps(properties) + ';application/json',
                                  'file': (f_name, f, mimetype)})
            watch = MultipartEncoderMonitor(e, lambda monitor: bar.update(monitor.bytes_read - bar.n))
            session.headers.update({"Content-Type": watch.content_type})
            bitstream = session.post(bitstream_url, data=watch, headers=headers, timeout=3)
    return bitstream


def create_metadata(field_name, value):
    new_list = []
    values = value.split('|')
    for single_value in values:
        entry = {"value": single_value,
                 "authority": None,
                 "language": None,
                 "confidence": -1}
        if field_name in fields_with_language:
            entry["language"] = "en"
        new_list.append(entry)
    return new_list


df = pd.read_csv(filename)
fields = list(df.columns)
fields.remove('bitstream')

all_items = []
for count, row in df.iterrows():
    print('')
    row = row.copy()
    dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
    name = row['dc.title']
    file_name = row['bitstream']
    print(count, name)
    # Format metadata to meet JSON requirements.
    item_data = {"name": name,
                 "inArchive": True,
                 "discoverable": True,
                 "withdrawn": False,
                 "type": "item"}
    metadata = {}
    for field in fields:
        field_value = row.get(field)
        if pd.notna(field_value):
            formatted_value = create_metadata(field, field_value)
            metadata[field] = formatted_value
    provNoteValue = "Submitted by " + user + " on " + dt + " (EST)."
    metadata["dc.description.provenance"] = [{'value': provNoteValue,
                                              "language": "en",
                                              "authority": None,
                                              "confidence": -1}]
    item_data["metadata"] = metadata

    # Create a new Item
    item = Item(item_data)
    new_item = d.create_item(parent=parent, item=item)
    if isinstance(new_item, Item) and new_item.uuid is not None:
        item_handle = new_item.handle
        row['item_created'] = True
        row['item_handle'] = item_handle
        row['item_uuid'] = new_item.uuid
        print(f'New item created! Handle: {item_handle}')
    else:
        row['item_created'] = False
        print('Error creating item.')
        all_items.append(row)
        continue

    # Create a new ORIGINAL bundle
    new_bundle = d.create_bundle(parent=new_item, name='ORIGINAL')
    if isinstance(new_bundle, Bundle) and new_bundle.uuid is not None:
        row['bundle_created'] = True
        row['bundle_uuid'] = new_bundle.uuid
        print(f'New bundle created! UUID: {new_bundle.uuid}')
    else:
        row['bundle_created'] = False
        print('Error creating bundle.')
        all_items.append(row)
        continue

    # Create and upload a new Bitstream.
    file_path = os.path.join(file_directory, file_name)
    mime_type = mimetypes.guess_type(file_path)
    new_bitstream = create_bitstream(new_bundle.uuid, file_name, mime_type, file_path)
    if new_bitstream.status_code == 201:
        bitstream_data = new_bitstream.json()
        bitstream_uuid = bitstream_data['id']
        print('New bitstream created! UUID: {}'.format(bitstream_uuid))
        row['bitstream_created'] = True
        row['bitstream_uuid'] = bitstream_uuid
    else:
        print('Error creating bitstream.')
        row['bitstream_created'] = False
    all_items.append(row)

dt = datetime.now().strftime('%Y-%m-%d')
log = pd.DataFrame.from_records(all_items)
log.to_csv('logOfItemsAddedTo_'+parent+'_'+dt+'.csv', index=False)

elapsedTime = time.time() - startTime
m, s = divmod(elapsedTime, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
