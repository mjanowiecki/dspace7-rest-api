import requests
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')


header = {'content-type': 'application/json'}
baseURL = 'https://j10p-clone.library.jhu.edu'

df = pd.read_csv(filename)

all_items = []
for count, row in df.iterrows():
    row = row
    bitstream_id = row['bitstream_id']
    print(count, bitstream_id)
    bitstream_endpoint = '/bitstreams/{}'.format(bitstream_id)
    full_link = baseURL+bitstream_endpoint
    r = requests.get(full_link, headers=header).json()


all_items = pd.DataFrame.from_dict(all_items)
all_items.to_csv('bitstream_metadata.csv', index=False)