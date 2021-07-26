import os
import logging
import pandas as pd
import os
import requests
from base64 import b64encode
import yaml as yml

import re
import sys
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from psycopg2.extras import execute_values

from airflow import DAG
from airflow import AirflowException

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

dag_default_args = {
    'owner': 'Hnin',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='cc_campaign_monitor_api_integration',
    default_args=dag_default_args,
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


### Initial 
### THis is to update accordingly    
apikey = "fz/ocDZn9rHm9ZAupGsDNjoBZGSnHO+8vmU/xWiPPvvFzPE0STJnTbnvu6/FvyOpUhp/TBu0txZ4xZYz6J8iyxkUDkBowVTiDflAvH0qdi5NzWSW+iq8L7j77/Qg77/6Wkj1JvxGkBTOtFOpV9saGw=="
clientId = "33604187fc197c87eb8d2f553935ef8e"

page = 1
pagesize = 1000

# Encode the credentials with base64
username = apikey
password = ''
encoded_credentials = b64encode(bytes(f'{username}:{password}',encoding='ascii')).decode('ascii')

# Use Basic Authorization
auth_header = f'Basic {encoded_credentials}'
print(f'Auth header: {auth_header}')

# Open the config file
with open(r'/opt/airflow/dags/dbconfig.yaml') as file:
    config = yml.full_load(file)

campaign_email_action_list = ["opens", "clicks", "unsubscribes", "bounces", "spam"]

# Base API parameters
params = {"page": page, "pagesize": pagesize, "orderfield": "email", "orderdirection": "asc" }

## The list of columns to select from the original API response. Only these columns will be stored into the Database
desired_columns = ["campaignId", "EmailAddress", "Date"]
#desired_columns = ["campaign_id", "email_address", "action_date"]

## Place holder to store the downloaded data
final_campaign_action_data = {"opens": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_opens"},
                            "clicks":{"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_clicks"},
                            "unsubscribes": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_unsubscribes"},
                            "bounces": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_bounces"},
                            "spam": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_spams"} }

####### Initialise the config related variable ######
# start time of processing
start_proc_time = datetime.now()

# Data loading method - "full" for the first time and "incremental" since second run
# Check loading configuration #METHOD: full/incremental
cfg_load_method = config['loading']['method']

#START_DATE_FULL_LOAD: starting date of data to be loaded (if method is full-load)
cfg_start_date = config['loading']['start_date_full_load']

#DAY_UPDATE_LAG: number of days of data lag
cfg_update_lag = config['loading']['day_update_lag']

# Calculate end date (the last latest day to extract/download data according to config 'cfg_update_lag')
cfg_end_date = datetime.now() - relativedelta(days = cfg_update_lag)

# number of days to consider a campaign is active
cfg_cmp_active_period_length = config['loading']['campaign_active_period_length']

# number of months to consider a campaign is active
default_full_load_month = config['loading']['default_full_load_month']

# Time to mark the end of processing
end_proc_time = 0

### Get the sent campaign
def getAllSentCampaigns_func(clientId):

    campaign_detail_url = f'https://api.createsend.com/api/v3.2/clients/{clientId}/campaigns.json'
    response = requests.get(campaign_detail_url, headers={'Authorization': auth_header } ) 
    response_campaign_list = response.json()
    df_campaign_list = pd.DataFrame.from_dict(response_campaign_list)

    print("length", df_campaign_list)
    return df_campaign_list


def set_query_start_date(tableName):
    query_startDate = get_last_success_load_time(tableName)
    return query_startDate

def get_last_success_load_time(table):
    '''
    This function queries the last success log of data insert for a particular table
    Argument:
    --------
    table: string
          It is the name of table such as
          "campaign_email_opens", "campaign_email_clicks", "campaign_email_bounces", "campaign_email_unsubscribes"
    '''
    
    ### Check the names of tables here TODO
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    load_log = pd.read_sql(  '  SELECT TOP 1'
                             '         load_date, '
                             '         method, '
                             '         data_start_date, ' 
                             '         data_end_date '
                             '  FROM campaign_monitor.log_success_load '
                             '  WHERE  table_name = \''+table+'\' '
                             '  ORDER BY end_runtime DESC ',
                          conn_ps)

    #if no data is available in the table (log_success_load), then no data has been load before
    #so, applying full load instead (4 months of data)    
    if (load_log.shape[0] == 0):
        cfg_start_date = datetime.now() - relativedelta(months = default_full_load_month)
        cfg_load_method = 'full [def]'
        print('#No data is available in the database. Applying \'Full Load\' From \''+cfg_start_date.strftime('%Y-%m-%d')+'\'')
    else:
        print("load_log.iloc[0]['data_end_date']", load_log.iloc[0]['data_end_date'])
        cfg_start_date = load_log.iloc[0]['data_end_date'] + relativedelta(days = 1)
        print(cfg_start_date)
        #cfg_start_date = datetime.strptime(load_log.iloc[0]['data_end_date'],'%Y-%m-%d %H:%M:%S').date() + relativedelta(days = 1)
        #cfg_start_date = datetime.strptime(load_log.iloc[0]['data_end_date'],'%Y-%m-%d') + relativedelta(days = 1) 
        print('#Applying \'Incremental Load\' From \''+load_log.iloc[0]['data_end_date'].strftime("%Y-%m-%d %H:%M:%S")+'\'')
    return cfg_start_date

def getFilteredActiveCampaign(df_campaign_list): 
    '''
    The function filters the campaigns which are still within n(for instance: 120) days since the email sent date.
    Basically, get all the still-active campaigns under monitoring period set by config
    '''
    # Get n() days ago from now
    last_active_date = get_datetime_Ndays_ago(cfg_cmp_active_period_length)
    
    # The campaign with SentDate which are greater than xxx, means within the monitoring period 
    # Get the index of campaigns running for less than n days 
    all_active_campaign = df_campaign_list['SentDate'] > last_active_date 
    
    # Filter out only the active campaigns
    campaigns_on_active_period = df_campaign_list.loc[all_active_campaign]
    
    return campaigns_on_active_period

def get_datetime_Ndays_ago(n):
    N_days_ago_datetime = datetime.now() - timedelta(days=n)
    return N_days_ago_datetime.strftime("%Y-%m-%d %H:%M:%S")

def get_next_NDays(dateFrom, n):
    ### Convert to datetime type to do date addition
    begindate = datetime.strptime(dateFrom, "%Y-%m-%d %H:%M:%S") 
    
    next_N_days_datetime = begindate + timedelta(days=n)
    ## return the string format of next N date
    return next_N_days_datetime.strftime("%Y-%m-%d %H:%M:%S")

def cal_number_of_days_to_campaingnEndDay(dateFrom, n):

     # Get the campaign end date    
    datetime_object = datetime.strptime(dateFrom, '%Y-%m-%d %H:%M:%S')
    next_N_days_datetime = datetime_object + timedelta(days=n)
    
    # If the campaign end date is already passed
    if (datetime.now() > next_N_days_datetime):
        end_day = next_N_days_datetime
        
    # If the campaign end date is in future, it should be n(2) days ago
    elif ( datetime.now() < next_N_days_datetime):
        end_day = get_datetime_Ndays_ago(cfg_update_lag)
        end_day = datetime.strptime(end_day, '%Y-%m-%d %H:%M:%S')

    # Calculate the difference between two dates
    number_of_days_required = end_day - datetime_object 
    return(number_of_days_required.days)

def insert_into_db(row, cursor, table):
    print("")
    # sql_insert_string = """ INSERT INTO %s  (campaign_id, email_address, action_date ) VALUES %s  """
    # values = { row['campaignId'], row['EmailAddress'], row['Date']}
    # execute_values(cursor, sql_insert_string , (table,  values))

    # cursor.execute(f'INSERT INTO {table}'
    #            '  (campaign_id, email_address, action_date ) VALUES   (?,?,?) ',
    #            row['campaignId'], row['EmailAddress'], row['Date'])


#########################################################
#
#   Get Some Campaign
#
#########################################################

def get_campaign_actions_func(**kwargs):


    ti = kwargs['ti']
    all_campaign_dict = ti.xcom_pull(task_ids=f'get_all_sent_campaign_task_id')

    df_all_sent_campaigns_list = pd.DataFrame(all_campaign_dict)

    print("df_all_sent_campaigns_list:", df_all_sent_campaigns_list)

    print("cfg_load_method", cfg_load_method)

    ###### Identify the requirement of load method ######
    # if Full, all the campaigns in the history will be downloaded
    # if Incremental, only active campaigns will be downloaded
    if (cfg_load_method == 'full'):
        campaigns_on_active_period = df_all_sent_campaigns_list
    elif (cfg_load_method == 'incremental'):
        campaigns_on_active_period = getFilteredActiveCampaign(df_all_sent_campaigns_list)

    print("campaigns_on_active_period", campaigns_on_active_period)
    # ###### Loop by each campaign
    for i in range(len(campaigns_on_active_period)) : 
        
        # Get the identifier of the campaign looping now
        campaign_startDate = campaigns_on_active_period.loc[i, 'SentDate']
        campaign_id = campaigns_on_active_period.loc[i, 'CampaignID'] 
        
        
        # Count the number of days between C-SentDate and End-Date
        number_of_days = cfg_cmp_active_period_length 
        
        ###### Loop by each action
        for action in campaign_email_action_list:
            
            ### Set the Query Start Date
            query_startDate = campaign_startDate
            if(cfg_load_method == 'incremental'):
                
                ## Set the load starting time according to the last loaded time recorded in the db-log
                query_startDate = set_query_start_date(final_campaign_action_data[action]['destination_table_name'])
                print("query_startDate", query_startDate)
                ## Overwriting number_of_days - if "incremental" to get the exact required count of loop
                number_of_days = cal_number_of_days_to_campaingnEndDay(campaign_startDate, cfg_cmp_active_period_length)
        
            ## Define the API End point  
            url_campaign_opens = f"https://api.createsend.com/api/v3.2/campaigns/{campaign_id}/{action}.json"
            # placeholder for each action's data
            df_campaign_email_action = pd.DataFrame()
            all_records = []
            
            #### Loop by each day for one action for one campaign
            for x in range(number_of_days):
                # Increase the next date count in every round of loop
                queryDate = get_next_NDays(query_startDate, x)
                # Update the API param "date"
                params["date"] = queryDate
                
                # active the GET request
                response = requests.get(url_campaign_opens, headers={'Authorization': auth_header }, params = params) 
                response_campaign_action = response.json()

                # logging.info("response_campaign_action length: ", len(response_campaign_action["Results"]) )
                print("length", len(response_campaign_action["Results"]))
                print("action", action)

                
                # If data is empty , skip the loop  
                if(len(response_campaign_action["Results"]) == 0): continue

                # Convert API response in dict format to Dataframe format
                response_dataframe = pd.DataFrame.from_dict(response_campaign_action["Results"])
                
                # Add "campaignId" value to the column
                response_dataframe["campaignId"] = campaign_id
                # Select the desired columns only from the entire response to save into the DB
                response_dataframe = response_dataframe[desired_columns]
                # Add up the dataframe to the result placeholder
                df_campaign_email_action = df_campaign_email_action.append(response_dataframe, ignore_index=True, sort=False) 
            
            if(len(df_campaign_email_action.index) > 0):           
                campaign_email_action_dataset = df_campaign_email_action.drop_duplicates()
                final_campaign_action_data[action]["data"] = final_campaign_action_data[action]["data"].append(campaign_email_action_dataset, ignore_index=True, sort=False) 
                # end_proc_time = datetime.now()
           
    
    return final_campaign_action_data

def insert_campaign_email_clicks_func(**kwargs):

    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    final_campaign_action_data = ti.xcom_pull(task_ids=f'get_campaign_actions_task_id')
    # print("campaign_action_data", final_campaign_action_data)
    # campaign_action_data_df = pd.DataFrame.from_dict(campaign_action_data_dict)

    #### Insert data by action (table by table)
     
    cursor = conn.cursor()
    # Name of table to store data
    table_name = final_campaign_action_data["clicks"]["destination_table_name"] 
    print("table_name", table_name)
    
    
    campaign_click_data = final_campaign_action_data["clicks"]['data']
    print("Check DATA TYPE====: ", type(campaign_click_data)) 
    # print("Columns: ", campaign_click_data.columns())
    # campaign_click_data_df = pd.DataFrame.from_dict(campaign_click_data)

    
    print("len", len(final_campaign_action_data['clicks']['data']))
 
    if (len(campaign_click_data) > 0):
        try:
            #if full-load, delete all the records from the table, log records from the log table
            if (cfg_load_method == 'full'):
                print("campaign_data", campaign_click_data)
                # TODO Later
                cursor.execute(""" DELETE FROM campaign_monitor.campaign_email_clicks """)
                cursor.execute(""" DELETE FROM campaign_monitor.log_success_load WHERE table_name LIKE %s ESCAPE ''""", (table_name,))    

            # print("campaign_click_data", campaign_click_data_df)
            # Rename the columns to sync with DB table columns
            campaign_click_data = campaign_click_data.rename(columns={"campaignId": "campaign_id", "EmailAddress": "email_address", "Date": "action_date"})

            logging.info("Column names ", campaign_click_data.columns)
            # campaign_click_records_values = campaign_click_data[desired_columns].to_dict('split')
            campaign_click_records_values = campaign_click_data.to_dict('split')
            values = campaign_click_records_values['data']

            # logging.info(values)

            # https://pynative.com/python-postgresql-insert-update-delete-table-data-to-perform-crud-operations/
     
            #insert dataframe to DB table
            sql_insert_string = """ INSERT INTO campaign_monitor.campaign_email_clicks (campaign_id, email_address, action_date ) VALUES %s """
            execute_values(cursor, sql_insert_string, values)
            end_proc_time = datetime.now()

            sql_insert_success_log = """ INSERT INTO  campaign_monitor.log_success_load  (load_date, method, data_start_date, data_end_date, start_runtime, end_runtime, table_name) VALUES %s """
            log_values = [[ start_proc_time.strftime('%Y-%m-%d'),
                cfg_load_method,
                cfg_start_date.strftime('%Y-%m-%d'),
                cfg_end_date.strftime('%Y-%m-%d'),
                start_proc_time,
                end_proc_time, 
                table_name
            ]]      
            execute_values(cursor, sql_insert_success_log, log_values)
        
            conn.commit()
        except:
            conn.rollback()
            print(sys.exc_info())
        finally:  
            cursor.close()
        ####### Close the connection #########
    conn.close()

    

# #########################################################
# #
# #   DAG Operator Setup
# #
# #########################################################

get_all_sent_campaign_task = PythonOperator(
    task_id=f'get_all_sent_campaign_task_id',
    python_callable=getAllSentCampaigns_func,
    provide_context=True,
    op_kwargs = {"clientId": clientId },
    dag=dag
)

get_campaign_actions_task = PythonOperator(
    task_id='get_campaign_actions_task_id',
    python_callable=get_campaign_actions_func,
    provide_context=True,
    dag=dag
)

insert_campaign_email_click_data = PythonOperator(
    task_id='insert_campaign_email_click_data_task_id',
    python_callable=insert_campaign_email_clicks_func,
    provide_context=True,
    dag=dag
)


get_all_sent_campaign_task  >> get_campaign_actions_task >> insert_campaign_email_click_data



