""" Output a CSV with data for all collections in the DSpace instance.
    CSV headers: id, uuid, name, handle
"""
import pandas as pd
from get_utils import get_paginated_data


def main():
    base_url = "https://j10p-stage.library.jhu.edu"
    endpoint = f"{base_url}/server/api/core/collections"
    timeout = 10
    size = 100

    all_items = []
    for page in get_paginated_data(endpoint, size, timeout):
        for collection in page["_embedded"]["collections"]:
            coll_dict = {
                "id": collection["id"],
                "uuid": collection["uuid"],
                "name": collection["name"],
                "handle": collection["handle"],
            }
            all_items.append(coll_dict)

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("allCollections.csv", index=False)


if __name__ == "__main__":
    main()
