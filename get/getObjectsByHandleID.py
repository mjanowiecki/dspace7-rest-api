import argparse
import requests
import pandas as pd


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


def json_to_flat_dict(item):
    """
    take JSON item with nested/multi-value fields returned from DSpace 7 API

    return a dict with a flat structure. where a field has multiple values,
    these values are concatenated separated by a pipe character.
    """
    item_dict = {"uuid": item["uuid"]}
    for fieldname, field_values in item["metadata"].items():
        str_value = "|".join([i["value"] for i in field_values])
        item_dict[fieldname] = str_value
    return item_dict


if __name__ == "__main__":
    main()
