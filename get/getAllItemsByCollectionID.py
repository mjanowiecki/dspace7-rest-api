""" provide UUID for a DSpace 7 collection or community
    output a CSV file with metadata for all items in that collection or community
"""

import pandas as pd
from get_utils import get_paginated_search_results, json_to_flat_dict


def main():
    # it looks like the way to retrieve items by collection in DSpace 7 is via the search endpoint
    # specify the collection or community UUID in the scope parameter
    # see https://groups.google.com/g/dspace-tech/c/jZ5eSMl1c1U

    # this example collection is the Baltimore City Sheet Maps collection
    # expecting 427 items
    collection_uuids = ["7e692697-d553-4887-8404-61a0821e42b8"]

    base_url = "https://j10p-stage.library.jhu.edu"
    timeout = 10
    size = 100

    all_items = []
    for uuid in collection_uuids:
        endpoint = f"{base_url}/server/api/discover/search/objects?scope={uuid}"
        for page in get_paginated_search_results(endpoint, size, timeout):
            results = page["_embedded"]["searchResult"]["_embedded"]["objects"]
            for result in results:
                item_dict = json_to_flat_dict(result["_embedded"]["indexableObject"])
                all_items.append(item_dict)

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("allCollectionItems.csv", index=False)



if __name__ == "__main__":
    main()
