import re, csv
from esgpull import Esgpull
from constants import pattern, csv_columns, query_sha
from utils import parse_file_json, get_facets_from_regex, get_files_from_esgpull_query

# regex prep
facets = set(re.findall(r'%\(([^()]*)\)s', pattern))
pattern = re.sub(r'%\(([^()]*)\)s', r'(?P<\1>[\\w-]+)', pattern)


def main():
    csv_lines = []
    for q in query_sha:
        print('now processing esgpull query {}'.format(str(q)))
        g = get_files_from_esgpull_query(q)
        # json object containing all the files linked to this query, basically an esgpull catalogue from the database
        files = g.asdict(files=True)
        # extracting file id and dataset id here from esgpull json dump
        extracted_info = parse_file_json(files)
        # for every file extracting relevant facets for the csv output
        for file in extracted_info:
            csv_lines.append(get_facets_from_regex(file, pattern))
        print("Files added to csv lines list.")

    print("All queries processed. Persisting on an output csv file for catalogue...")
    output_file = "output.csv"

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for index, data in enumerate(csv_lines, start=0):
            data["index"] = index
            writer.writerow(data)
    # todo add dataset complete logic mainly for RUCIO if not the intake catalogue, need to loop with Fabrizio over that

if __name__ == '__main__':
    main()
