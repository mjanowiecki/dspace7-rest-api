import pandas as pd
from get_utils import get_paginated_data
from datetime import datetime

scriptStart = datetime.now()

df = pd.read_csv('test.csv')


def main():

    base_url = 'https://jscholarship.library.jhu.edu'
    timeout = 10
    size = 100

    all_items = []
    for count, row in df.iterrows():
        bundle_uuid = row['bundle_uuid']
        endpoint = f"{base_url}/server/api/core/bundles/{bundle_uuid}/bitstreams"
        for page in get_paginated_data(endpoint, size, timeout):
            bitstreams = page['_embedded']['bitstreams']
            for bitstream in bitstreams:
                log = row.copy()
                bitstream_uuid = bitstream['uuid']
                bitstream_name = bitstream['name']
                log['bitstream_uuid'] = bitstream_uuid
                log['bitstream_name'] = bitstream_name
                all_items.append(log)

    all_items = pd.DataFrame.from_records(all_items)
    all_items.to_csv('originalBitstreamsByBundleIDs.csv', index=False)


if __name__ == "__main__":
    main()

print(datetime.now() - scriptStart)
