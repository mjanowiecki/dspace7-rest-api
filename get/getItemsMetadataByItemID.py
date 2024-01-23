""" This script gets item metadata from the DSpace 7 API and outputs it in CSV format.

    As input, it takes a CSV file that either:
        * lists one or more item UUIDs in a column labeled "uuid", OR
        * lists one or more item handles in a column labeled "handle"

    If both identifiers are present, "uuid" will be used.

"""

import argparse
import requests
import pandas as pd
from get_utils import json_to_flat_dict

BASE_URL = "https://j10p-stage.library.jhu.edu"
TIMEOUT = 10


def main():
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file")
    args = parser.parse_args()
    if args.file:
        filename = args.file
    else:
        # prompt user to enter filename if not provided as arg
        filename = input("Enter filename (including '.csv'): ")

    # iterate over input csv
    df = pd.read_csv(filename)
    all_items = []
    for index, row in df.iterrows():
        if row.get("uuid"):
            item_id = row["uuid"]
            r = get_by_uuid(item_id, BASE_URL)
        elif row.get("handle"):
            item_id = row["handle"]
            r = get_by_handle(item_id, BASE_URL)
        if r.status_code == 200:
            print(index, item_id)
            item_dict = json_to_flat_dict(r.json())
            all_items.append(item_dict)
        # TODO: error handling for timeouts and non-200 statuses

    # output csv
    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("item_metadata.csv", index=False)


def get_by_uuid(uuid, base_url=BASE_URL, timeout=TIMEOUT):
    """retrieve a DSpace item by its uuid
    returns requests object
    """
    return requests.get(f"{base_url}/server/api/core/items/{uuid}", timeout=timeout)


def get_by_handle(handle, base_url=BASE_URL, timeout=TIMEOUT):
    """retrieve a DSpace item by its handle identifier
    returns requests object
    """
    return requests.get(
        f"{base_url}/server/api/pid/find", params={"id": handle}, timeout=timeout
    )


if __name__ == "__main__":
    main()
