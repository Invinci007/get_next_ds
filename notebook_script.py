# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku_logger import DataikuLogger
from datetime import datetime
import time
# Assuming incremental_helper is a custom module. If it's not available in the environment,
# this line might cause an error later. For now, include it as per the original script.
from incremental_helper import get_current_eastern_time_formatted

sqlth_ds = dataiku.Dataset('sqlth_partitions_stacked')
sqlth_df = sqlth_ds.get_dataframe()

max_ds = dataiku.Dataset('ANCILLARY_RAW_MAX_TSTAMPS')
max_df = max_ds.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
sqlth_df.head()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
client = dataiku.api_client()
project = client.get_project(dataiku.default_project_key())
logger = DataikuLogger(metadata='ignition, historical, ancillary ingest').get_logger()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# get the valid tag value ids
def get_tag_qry(site):
    bnj_tag_qry = """INNER JOIN ignition_historian_NJ.sqlth_te on tagid=id
    WHERE tagpath LIKE '%swiftsensors%'
    OR tagpath LIKE '%_set' OR tagpath LIKE '%setpoint%' OR tagpath like '%oducl%'or tagpath like '%odlcl%'"""
    asc_tag_qry = """INNER JOIN ignition_historian.sqlth_te on tagid=id
    WHERE tagpath LIKE '%swiftsensors%'
    OR tagpath LIKE '%_set' OR tagpath LIKE '%setpoint%' OR tagpath like '%oducl%'or tagpath like '%odlcl%'"""
    csc_tag_qry = """INNER JOIN ignition_historian_csc.sqlth_te on tagid=id
    WHERE tagpath LIKE '%swiftsensors%'
    OR tagpath LIKE '%_set' OR tagpath LIKE '%setpoint%' OR tagpath like '%oducl%'or tagpath like '%odlcl%'"""

    if site == 'CSC':
        return csc_tag_qry
    else:
        if site == 'BNJ':
            return bnj_tag_qry
        else:
            if site == 'ASC':
                return asc_tag_qry

    raise Exception(f'Site {site} not found')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
tables_processed = []

def get_next_ds(site):
    """
    Determine the next dataset to process based on a given site.

    Args:
        site (str): The name of the site to filter datasets.

    Returns:
        tuple: A tuple containing the next dataset name (`pname`) and its maximum ingested timestamp (`max_ingested_date`).
               If no dataset is found, returns an empty string and 0.
    """

    for t in sqlth_df.itertuples(): # Iterate over rows in the DataFrame as named tuples
        if site.lower() in t.original_dataset: # Check if the site matches the original dataset
            tbl = t.original_dataset + '_' + t.pname # Construct the table name by combining dataset and process name

            # Check if the table has not been processed and matches the pattern '_1_' in the name
            if not tbl in tables_processed and '_1_' in t.pname :
                tables_processed.append(tbl) # Mark the table as processed

                max_tstamp = 0
                try:
                    # look for the max t_stamp we've ever seen for this table
                    # This is the line that needs max_df
                    max_tstamp = max_df.query(f'original_dataset=="{t.pname}" & SITE=="{site}"').iloc[0]['t_stamp_max']
                except:
                    # Log if no timestamp exists for the current process name
                    logger.info(f'no timestamp for {t.pname}')

                # what is the max date for this table as reported by sqlth
                max_reported_source_date = datetime.fromtimestamp(t.end_time / 1000)

                # what is the max date we have seen
                max_ingested_date = datetime.fromtimestamp(max_tstamp / 1000)

                # how many days ago is the last date reported by sqlth
                days_ago = (datetime.now() - max_reported_source_date).total_seconds() / 60 / 60 / 24

                # don't sync if we've synced records for this table and the table max records ended 5 days ago
                if not (max_tstamp > 0 and (days_ago > 5)): # we've already pulled these
                    return t.pname, max_ingested_date

    return '', 0

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# clear incremental datasets (these are append mode)
for st in ['CSC', 'BNJ', 'ASC']:
    raw_ds = project.get_dataset(f'{st}_ANCILLARY_RAW')
    raw_ds.clear()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
runs = []
while True:
    jobs = []
    for site in ['CSC', 'BNJ', 'ASC']:
        try:
            tbl, max_tstamp = get_next_ds(site)

            if tbl == '': # done for this site
                continue

            logger.info(f'processing {site}, {tbl}')

            raw_ds = project.get_dataset(f'{site}_ANCILLARY_RAW')
            dyn_ds = project.get_dataset(f'GENERIC_ANCILLARY_SOURCE_{site}')

            source_settings = dyn_ds.get_settings()
            source_params = source_settings.get_raw_params() #Get the parameters settings necessary for query mode

            # create the query for extract
            key_qry = get_tag_qry(site)

            # set the query for the source mySQL table
            qry = f'''SELECT ign.*, '{site}' as 'SITE', '{tbl}' as 'original_dataset', '${{ANCILLARY_SYNC_DATE}}' as "INSERT_DATE"
            FROM {tbl} ign
            {key_qry} and t_stamp>'{max_tstamp}'
            '''

            source_params['query'] = qry
            source_settings.save()

            # start the job to move this data to the raw-incremental. don't wait for it to finish
            job = raw_ds.build(job_type="RECURSIVE_BUILD", no_fail=True, wait=False)
            jobs.append(job)

            runs.append({'site': site, 'table': tbl, 't_stamp_start': max_tstamp, 'run_date': get_current_eastern_time_formatted()})
        except Exception as e:
            logger.exception(e)
#             print(e)

    if len(jobs) == 0:
        break # there are no more jobs, we're all done
    else:
        while True:
            done = True
            for j in jobs:
                state = j.get_status()['baseStatus']['state']
                if not (state == 'DONE' or state == 'FAILED'):
                    done = False # jobs are still running

            if done: # jobs have all completed, queue the next batch
                break

            time.sleep(1) # wait 1s before checking status again

# build all the way to raw, including the group
raw_ds = project.get_dataset(f'ANCILLARY_RAW_MAX_TSTAMPS')

logger.info('building to raw')
job = raw_ds.build(job_type='RECURSIVE_BUILD', wait=True)
logger.info('building complete')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
logger.flush()

# Write recipe outputs
INGEST_RUN_RESULTS = dataiku.Dataset("ANCILLARY_INC_RESULTS")
INGEST_RUN_RESULTS.write_with_schema(pd.DataFrame.from_dict(runs))
