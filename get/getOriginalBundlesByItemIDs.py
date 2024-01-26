import pandas as pd
from get_utils import get_paginated_data
from datetime import datetime

scriptStart = datetime.now()

df = pd.read_csv('allCollectionItems.csv')


def main():

    base_url = 'https://jscholarship.library.jhu.edu'
    timeout = 10
    size = 100

    all_items = []
    for count, row in df.iterrows():
        item_uuid = row['uuid']
        endpoint = f"{base_url}/server/api/core/items/{item_uuid}/bundles"
        print(count, item_uuid)
        for page in get_paginated_data(endpoint, size, timeout):
            bundles = page['_embedded']['bundles']
            for bundle in bundles:
                log = row.copy()
                bundle_name = bundle['name']
                if bundle_name == 'ORIGINAL':
                    bundle_uuid = bundle['uuid']
                    log['bundle_name'] = bundle_name
                    log['bundle_uuid'] = bundle_uuid
                    all_items.append(log)

    all_items = pd.DataFrame.from_records(all_items)
    all_items.to_csv('originalBundlesByItemIDs.csv', index=False)


if __name__ == "__main__":
    main()

print(datetime.now() - scriptStart)
