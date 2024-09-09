import pandas as pd
import argparse
import time
import secret
from authenticateToDSpace import authenticate_to_dspace
from datetime import datetime

startTime = time.time()

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

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


fields_to_keep = ['dc.identifier.uri', 'dc.date.accessioned',
                  'dc.date.available', 'dc.description.provenance']


field_string = ['dc.title', 'dc.title.alternative', 'dc.description',
                'dc.type', 'dc.language']

field_authority = ['dc.publisher', 'dc.contributor.author',
                   'dc.contributor', 'dc.contributor.editor',
                   'dc.contributor.illustrator']

field_string_authority = ['dc.subject', 'dc.publisher.country',
                          'dc.rights']


def add_field(field_name, metadata_value):
    list_of_entries = []
    values = metadata_value.split('|')
    for value in values:
        field_entry = {}
        if field_name in field_string:
            value_lang = value.split(';;')
            value_string = value_lang[0]
            language_code = value_lang[1]
            value_string = value_string.strip()
            field_entry['value'] = value_string
            field_entry['language'] = language_code
            field_entry['authority'] = None
        elif field_name in field_authority:
            value_authority = value.split(';;')
            value_string = value_authority[0]
            authority = value_authority[1]
            value_string = value_string.strip()
            field_entry['value'] = value_string
            field_entry['language'] = None
            field_entry['authority'] = authority
        elif field_name in field_string_authority:
            value_string_authority = value.split(';;')
            value_string = value_string_authority[0]
            language_code = value_string_authority[1]
            authority = value_string_authority[2]
            value_string = value_string.strip()
            field_entry['value'] = value_string
            field_entry['language'] = language_code
            field_entry['authority'] = authority
        else:
            value = value.strip()
            field_entry['value'] = value
            field_entry['language'] = None
            field_entry['authority'] = None
        list_of_entries.append(field_entry)
    metadata[field_name] = list_of_entries


df = pd.read_csv(filename)
column_list = df.columns.tolist()
column_list.remove('dspace_item_id')

all_items = []
for count, row in df.iterrows():
    s = authenticate_to_dspace(base_url, user, password)
    headers = s.headers
    metadata = {}
    uuid = row['dspace_item_id']
    item_log = {'uuid': uuid}
    print(count, uuid)
    item_url = f"{base_url}/core/items/{uuid}"
    item_log['url'] = item_url
    item_json = s.get(item_url)
    if item_json.status_code == 200:
        item_log['item_downloaded'] = True
        print('item download successful')
        item_json = item_json.json()
        for field in fields_to_keep:
            metadata_section = item_json['metadata']
            field_value = metadata_section.get(field)
            if field_value:
                metadata[field] = field_value
        for column_name in column_list:
            data = row[column_name]
            if not pd.isnull(data):
                add_field(column_name, data)
        del item_json['lastModified']
        del item_json['_links']
        dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
        provNoteValue = "Metadata updated by " + user + " on " + dt + " (EST)."
        provenance = metadata['dc.description.provenance']
        provenance.append({'value': provNoteValue,
                           'language': 'en',
                           'authority': None,
                           'confidence': -1})
        metadata['dc.description.provenance'] = provenance
        item_json['metadata'] = metadata
        updated_json = s.put(item_url, json=item_json, headers=headers)
        print(updated_json)
        if updated_json.status_code == 200:
            item_log['item_updated'] = True
            print('item update successful')
        else:
            item_log['item_updated'] = False
            print('item update failed')
    else:
        item_log['item_downloaded'] = False
        print('item download failed')
    all_items.append(item_log)


# Output item log as CSV
all_items = pd.DataFrame.from_records(all_items)
dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
all_items.to_csv('item_metadata_log_'+dt+'.csv', index=False)
