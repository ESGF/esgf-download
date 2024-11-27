# some constants
query_sha = ["aad1bc"]
csv_columns = ['index','activity_id','institution_id','source_id','experiment_id','member_id','table_id','variable_id',
               'grid_label','time_range','start_year','end_year','path']
pattern = ('%(mip_era)s.%(activity_id)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.'
           '%(variable_id)s.%(grid_label)s.%(version)s.%(file_drs)s')
# period_pattern = "[a-zA-Z0-9]*\_(\d{8})-(\d{8})"
period_pattern = r"[a-zA-Z0-9]*_(\d{8})-(\d{8})"
esgpull_json_file = "esgpull_catalogue.json"
enesds_catalogue = "ENESDS_CMIP6_OCT24.csv"
datapath_prefix = "/home/jovyan/data/"

RSE = "DESY-DCACHE"
SCOPE = "abennasser"
