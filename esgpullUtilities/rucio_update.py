import rucio
from esgpull import Esgpull
from esgpull.models.sql import query

from utils import parse_file_json, get_files_from_esgpull_query
from constants import query_sha, RSE, SCOPE


def attach_datasets_to_rucio(datasets):
    dataset = rucio.add_dataset(scope=SCOPE, name="<DATASET>", rse=RSE)
    attachment = rucio.attach_dids(
        scope=SCOPE,
        name="<DATASET>",
        dids=[
            {"scope": "<SCOPE>", "name": "<FILENAME1>"},
            {"scope": "<SCOPE>", "name": "<FILENAME2>"},
        ],
        rse="<RSE>",
    )

def main():
    """
    Will grab the dataset from esgpull resulted json as well as their contents in files
    Every item in the list will then be attached to the rucio instance.
    """
    for q in query_sha:
        print('now attaching datasets from esgpull query {}...'.format(str(q)))
        g = get_files_from_esgpull_query(q)
        files = g.asdict(files=True)
        # dataset id and file id are in this list of dictionary items.
        extracted_info = parse_file_json(files)
        for item in extracted_info:
            attach_datasets_to_rucio(item['dataset_id'])



if __name__ == '__main__':
    main()
