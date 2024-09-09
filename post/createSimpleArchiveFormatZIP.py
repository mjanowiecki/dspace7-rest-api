import pandas as pd
import os
from xml.etree import ElementTree as ET
import argparse
import zipfile

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

archive_directory = '/Users/michelle/Desktop/archive_directory'
zip_file = '/Users/michelle/Desktop/items.zip'
current_bitstream_directory = '/Users/michelle/Desktop/science_review'

df = pd.read_csv(filename)
fields = list(df.columns)
fields.remove('bitstream')
fields.remove('collection')


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
        if len(field_components) == 3:
            qualifier = field_components[2]
        else:
            qualifier = "none"
        attributes = {'element': element,
                      'qualifier': qualifier}
        field_value = item[field]
        if pd.notna(field_value):
            field_value = field_value.split('|')
            for value in field_value:
                if '@' in value:
                    value_with_language = value.split('@')
                    value = value_with_language[0]
                    language = value_with_language[1]
                    attributes['language'] = language
                    field = ET.SubElement(metadata, 'dcvalue', attrib=attributes)
                    field.text = value
                else:
                    field = ET.SubElement(metadata, 'dcvalue', attrib=attributes)
                    field.text = value
    file_location = item_folder+'/dublin_core.xml'
    tree = ET.ElementTree(metadata)
    with open(file_location, 'wb') as f:
        tree.write(f, encoding='UTF-8')


def create_text_file(folder, text_name, item, column_name):
    value = item[column_name]
    text_file_name = folder+'/'+text_name
    with open(text_file_name, 'w') as file:
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
    current_location = current_bitstream_directory+'/'+bitstream
    new_location = item_folder+'/'+bitstream
    if os.path.exists(new_location):
        pass
    else:
        try:
            os.path.exists(current_location)
            os.rename(current_location, new_location)
        except FileNotFoundError:
            print(current_location+'not found')

    print(os.listdir(item_folder))

new_zip = zipfile.ZipFile(zip_file, 'w')
new_zip.write(archive_directory)
new_zip.close()
