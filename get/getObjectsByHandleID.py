import argparse
import requests
import pandas as pd
from get_utils import json_to_flat_dict

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

    # set base url, endpoint, and timeout
    base_url = "https://j10p-stage.library.jhu.edu"
    endpoint = "server/api/pid/find"
    timeout = 10

    # iterate over input csv
    df = pd.read_csv(filename)
    all_items = []
    for index, row in df.iterrows():
        print(index, row["handle"])
        # get object data
        r = requests.get(
            f"{base_url}/{endpoint}", timeout=timeout, params={"id": row["handle"]}
        )
        if r.status_code == 200:
            item_dict = json_to_flat_dict(r.json())
            all_items.append(item_dict)
        # TODO: error handling for timeouts and non-200 statuses

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("handle_metadata.csv", index=False)


if __name__ == "__main__":
    main()
