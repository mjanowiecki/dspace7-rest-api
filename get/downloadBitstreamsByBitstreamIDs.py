import pandas as pd
from datetime import datetime
import requests
import os
import shutil

scriptStart = datetime.now()

df = pd.read_csv('missing.csv')

destination_folder = '/Users/michelle/Desktop/levy_files'

if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)


def main():

    base_url = 'https://jscholarship.library.jhu.edu'

    for count, row in df.iterrows():
        bitstream_uuid = row['bitstream_uuid']
        bitstream_name = row['bitstream_name']
        print(count, bitstream_name)
        file_path = os.path.join(destination_folder, bitstream_name)
        endpoint = f"{base_url}/bitstreams/{bitstream_uuid }/download"
        r = requests.get(endpoint, stream=True)
        if r.ok:
            with open(file_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            print('Download succeeded')
        else:  # HTTP status code 4XX/5XX
            print("Download failed: status code {}\n{}".format(r.status_code, r.text))
        print('')


if __name__ == "__main__":
    main()

print(datetime.now() - scriptStart)
