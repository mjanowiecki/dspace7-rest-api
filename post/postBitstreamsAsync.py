import mimetypes
import secret
import pandas as pd
import argparse
import aiohttp
import asyncio
import json
from datetime import datetime
import time

scriptStart = datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

secrets_version = input('To edit production server, enter secrets file: ')
if secrets_version != '':
    try:
        secret = __import__(secrets_version)
        print('Editing Production')
    except ImportError:
        print('Editing Stage')
else:
    print('Editing Stage')

base_url = secret.base_url
user = secret.user
password = secret.password

file_directory = '/Users/michelle/Desktop/science_review/'


# Read CSV into DataFrame and create full link for each item in new column 'bundle_uuid.'
df = pd.read_csv(filename)
df['bitstream_url'] = base_url+'/core/bundles/'+df['bundle_uuid'].astype(str)+'/bitstreams'

# Create list of dictionaries with link and filename.
all_bitstreams = []
for count, row in df.iterrows():
    bitstream_url = row['bitstream_url']
    file_name = row['bitstream']
    file_location = file_directory+file_name
    mimetype = mimetypes.guess_type(file_location)[0]
    bit_info = {'bitstream_url': bitstream_url,
                'file_name': file_name,
                'file_location': file_location,
                'mimetype': mimetype}
    all_bitstreams.append(bit_info)

total_bitstreams = len(all_bitstreams)


# Split list into batches of 100 bitstreams.
d = {}
for i, x in enumerate(all_bitstreams):
    d.setdefault(i // 5, []).append(x)
bitstream_batches = list(d.values())
total_batches = len(bitstream_batches)
print('Total items {}, split into {} batches.'.format(total_bitstreams, total_batches))


def prepare_form_data(bitstream_dict):
    bitstream_link = bitstream_dict['bitstream_url']
    f_name = bitstream_dict['file_name']
    file_path = bitstream_dict['file_location']
    mime_type = bitstream_dict['mimetype']
    # Create multipart form data
    properties = {'name': f_name,
                  'metadata': {'dc.description': [{'value': 'Bitstream uploaded by Python REST Client',
                                                   'language': 'en',
                                                   'authority': None,
                                                   'confidence': -1,
                                                   'place': 0}]},
                  'bundleName': 'ORIGINAL'}

    form_data = aiohttp.FormData()
    form_data.add_field('file', open(file_path, 'rb'), filename=f_name, content_type=mime_type)
    form_data.add_field('properties', json.dumps(properties), content_type='application/json')
    return form_data, bitstream_link


async def post_bitstream(session, new_headers, form_info):
    form_data = form_info[0]
    bitstream_link = form_info[1]
    new_headers['Content-Encoding'] = 'gzip'
    async with session.post(bitstream_link, data=form_data, headers=new_headers) as post_response:
        await post_response.status
        if post_response.status == 201:
            result = post_response.json()
        else:
            result = None
    data = {'link': bitstream_link, 'result': result}
    return data


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url+'/api/authn/status') as status:
            token = status.headers.get('DSPACE-XSRF-TOKEN')
            session_cookies = {'X-XSRF-Token': token}
            session_headers = {'User-Agent': 'DSpace Python REST Client',
                               'X-XSRF-TOKEN': token}
            params = {'user': user, 'password': password}
        async with session.post(base_url+'/authn/login', data=params, cookies=session_cookies,
                                headers=session_headers) as login:
            if login.status == 200:
                print('Logged in to DSpace')
            else:
                print(login.status)
            login_header = login.headers
            auth_token = login_header.get('Authorization')
        async with session.patch(base_url+'/api/authn/status') as status_check:
            updated_cookie = status_check.cookies.get("DSPACE-XSRF-COOKIE").value
            new_headers = {
                "Authorization": auth_token,
                "X-XSRF-TOKEN": updated_cookie}
        bitstream_forms = [prepare_form_data(bit_dict) for bit_dict in batch]
        # Loop through links in item_links and retrieve JSON via API.
        bitstream_posts = [post_bitstream(session, new_headers, form) for form in bitstream_forms]
        # Gather responses from items.
        responses = await asyncio.gather(*bitstream_posts)
        # Loop through responses.
        for response in responses:
            response_json = response['result']
            if response_json:
                bitstream_uuid = response['id']
            else:
                bitstream_uuid = None
            item_log = {'link': response['link'], 'bitstream_uuid': bitstream_uuid}
            # Add log to list 'item_logs'.
            item_logs.append(item_log)


for batch_count, batch in enumerate(bitstream_batches):
    total_items_in_batch = str(len(batch))
    startTime = datetime.now()
    batch_count = batch_count + 1
    string_batch_count = str(batch_count)
    if batch_count >= 1:
        time.sleep(2)
        item_logs = []
        # Run main function.
        print('')
        print('Batch {} of {}, containing {} items.'.format(string_batch_count, total_batches, total_items_in_batch))
        print('')
        asyncio.run(main())

        # Convert log to DataFrames.
        item_logs = pd.DataFrame.from_records(item_logs)
        dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
        new_filename = 'log_batch_' + string_batch_count.zfill(2) + '_' + dt + '.csv'
        # Create CSV using DataFrame log.
        item_logs.to_csv(new_filename, index=False)
        print(datetime.now() - startTime)

print(datetime.now() - scriptStart)

