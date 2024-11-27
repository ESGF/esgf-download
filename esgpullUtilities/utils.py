import re, os
from esgpull import Esgpull
from constants import period_pattern, datapath_prefix



def parse_file_json(json_input_object):
    """
    Takes into input the query files dump into json format, extracts relevant info in preparation of
    writing CSV lines
    :param json_input_object:
    :return:
    """

    extracted_info = []
    files_list = list(json_input_object.values())[0]['files']
    for file in files_list:
        file_id = file['file_id']
        dataset_id = file['dataset_id']
        local_path = os.path.join(datapath_prefix, file['local_path'])
        if file_id and dataset_id:
            extracted_info.append({'file_id': file_id, 'dataset_id': dataset_id, 'local_path': local_path})
    return extracted_info


def get_facets_from_regex(extracted_info, pattern):
    match = re.match(pattern, extracted_info['file_id'].lower())
    if match:
        facets = match.groupdict()
        period = re.findall(period_pattern, facets['file_drs'])
        if period:
            facets['start_year'] = period[0][0]
            facets['end_year'] = period[0][1]
            facets['time_range'] = facets['start_year'] + '-' + facets['end_year']
        else:
            facets['start_year'] = 'NA'
            facets['end_year'] = 'NA'
            facets['time_range'] = 'NA'
        # manually adding local path from esgpull dump
        facets['path'] = extracted_info['local_path']
        del facets['file_drs']
        del facets['version']
        del facets['mip_era']
        return facets
    else:
        print(f"No match found for file_id: {extracted_info['file_id']}")
        return {}


def get_files_from_esgpull_query(query_sha):
    # init esg interface
    esg = Esgpull()
    query = esg.graph.get(query_sha)
    graph = esg.graph.subgraph(query)
    return graph
