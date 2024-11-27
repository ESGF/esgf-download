import rucio
import json


from utils import parse_file_json, get_files_from_esgpull_query
from constants import RSE, SCOPE


def get_dataset_list(file_path):
    """
    Reads JSON file, parses dictionary and lists dataset identifiers with the attached files.
    Returns: Dictionary with key as dataset_id and value as list of files.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)

    dataset_dict = {}
    data = data['49d8b79df01e29fa065ce9d65211d03e98b19750']['files']
    for file in data:
        dataset_id = file['dataset_id']
        file_id = file['file_id']
        if dataset_id in dataset_dict.keys():
            files = dataset_dict[dataset_id]
            files.append(file_id)
        else:
            files = [file_id]
        dataset_dict[dataset_id] = files

    print(dataset_dict)
    return dataset_dict


def attach_datasets_to_rucio(dataset_id, files):
    dataset = rucio.add_dataset(scope=SCOPE, name=dataset_id, rse=RSE)
    dids = []
    for file in files:
        file_dic = {'scope': SCOPE, 'name': file}
        dids.append(file_dic)

    attachment = rucio.attach_dids(
        scope=SCOPE,
        name=dataset_id,
        dids=dids,
        rse="<RSE>",
    )


def main():
    """
    Will grab the dataset from esgpull resulted json as well as their contents in files
    Every item in the list will then be attached to the rucio instance.
    """
    print("Retrieving dataset/file dictionary...")
    dataset_dict = get_dataset_list('subset_rucio_cmcc.json')
    print("Processing dictionary items...")
    for key, value in dataset_dict:
        print('now attaching datasets from file {}...'.format(key))
        attach_datasets_to_rucio(key, value)
        print('Dataset has {} files attached.'.format(len(value)))
        print('----------------------------------------')




if __name__ == '__main__':
    main()
