import pandas as pd
import os
from xml.etree import ElementTree as ET
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

archive_directory = '/Users/michelle/Desktop/archive_directory'

df = pd.read_csv(filename)
fields = list(df.columns)
fields.pop('bitstream', 'collection')


def create_item_folder(item_number):
    new_folder = archive_directory+'/'+item_number
    if not os.path.exists(new_folder):
        os.makedirs(new_folder)
        return new_folder


def create_xml_file(list_of_fields, item):
    metadata = ET.Element('dublin_core')
    for field in list_of_fields:
        field_components = field.split('.')
        element = field_components[1]
        if len(field_components) == 2:
            qualifier = field_components[2]
        else:
            qualifier = "none"
        attributes = {'element': element,
                      'qualifier': qualifier}
        field_value = item['field']
        if pd.notna(field_value):
            field_value = field_value.split('|')
            for value in field_value:
                field = ET.SubElement(metadata, 'dcvalue', attrib=attributes)
                field.text = value
    file_location = item_folder+'/dublin_core.xml'
    metadata.write(file_location, encoding='UTF-8')


def create_text_file(folder, text_name, item, column_name):
    value = item[column_name]
    file = open(folder+'/'+text_name, 'a')
    file.write(value)
    file.close()


for count, row in df.iterrows():
    string_item_num = str(count+1).zfill(3)
    item_folder = create_item_folder(string_item_num)
    create_xml_file(fields, row)
    create_text_file(item_folder, 'contents.txt', row, 'bitstream')
    create_text_file(item_folder, 'collections.txt', row, 'collection')
    # Move bitstreams into item_folder.
    bitstream = row['bitstream']
    current_location = '/Users/michelle/Desktop/archive_directory/bitstreams/' + bitstream
    item_folder_location = item_folder+'/'+bitstream
    os.rename(current_location, item_folder_location)


