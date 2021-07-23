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
from psycopg2 import sql

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
    dag_id='campaign_monitor_api_integration',
    default_args=dag_default_args,
    schedule_interval="@weekly",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


### Initial 
### THis is to update accordingly    
apikey = "xxx"
clientId = "xxx"

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

#########################################################
#
#   Get Some Campaign Email User Action Data
#
#########################################################

def get_campaign_action_data_func(**kwargs):


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
                
    return final_campaign_action_data

#########################################################
#
#   Insert Campaign Email User Action Data into Data Warehouse
#
#########################################################
def insert_campaign_email_action_data_func(sql_del_str, sql_insert_string, action, **kwargs):

    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    final_campaign_action_data = ti.xcom_pull(task_ids=f'get_campaign_email_user_actions_data')
     
    cursor = conn.cursor()
    # Name of table to store data
    table_name = final_campaign_action_data[action]["destination_table_name"] 
    print("table_name", table_name)
     
    campaign_click_data = final_campaign_action_data[action]['data']
    print("Check DATA TYPE====: ", type(campaign_click_data))  
    print("len", len(final_campaign_action_data[action]['data']))
 
    if (len(campaign_click_data) > 0):
        try:
            #if full-load, delete all the records from the table, log records from the log table
            if (cfg_load_method == 'full'):
                print("campaign_data", campaign_click_data)
                
                cursor.execute(sql_del_str)
                cursor.execute(""" DELETE FROM campaign_monitor.log_success_load WHERE table_name LIKE %s ESCAPE ''""", (table_name,)) 

            # Rename the columns to sync with DB table columns
            campaign_click_data = campaign_click_data.rename(columns={"campaignId": "campaign_id", "EmailAddress": "email_address", "Date": "action_date"})

            logging.info("Column names ", campaign_click_data.columns)
            # Original Data in Dataframe was passed in Dictionary format in Airflow, that's why split and extact by "data"
            campaign_click_records_values = campaign_click_data.to_dict('split')
            values = campaign_click_records_values['data']

            #insert dataframe to DB table
            execute_values(cursor, sql_insert_string, values)
            end_proc_time = datetime.now()

            # Create the log of successful data load
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

extract_all_sent_campaign_task = PythonOperator(
    task_id=f'get_all_sent_campaign_list_data',
    python_callable=getAllSentCampaigns_func,
    provide_context=True,
    op_kwargs = {"clientId": clientId },
    dag=dag
)

extract_campaign_user_email_actions_data = PythonOperator(
    task_id='get_campaign_email_user_actions_data',
    python_callable=get_campaign_action_data_func,
    provide_context=True,   
    dag=dag
)

sql_del_str_clicks = """ DELETE FROM campaign_monitor.campaign_email_clicks """
sql_insert_str_clicks = """ INSERT INTO campaign_monitor.campaign_email_clicks (campaign_id, email_address, action_date ) VALUES %s """
insert_campaign_email_click_data = PythonOperator(
    task_id='insert_campaign_email_click_data',
    python_callable=insert_campaign_email_action_data_func,
    provide_context=True,
    op_kwargs = {"action": "clicks", 
                "sql_insert_string" : sql_insert_str_clicks,
                "sql_del_str": sql_del_str_clicks},
    dag=dag
)

sql_del_str_opens = """ DELETE FROM campaign_monitor.campaign_email_opens """
sql_insert_str_opens = """ INSERT INTO campaign_monitor.campaign_email_opens (campaign_id, email_address, action_date ) VALUES %s """
insert_campaign_email_open_data = PythonOperator(
    task_id='insert_campaign_email_open_data',
    python_callable=insert_campaign_email_action_data_func,
    provide_context=True,
    op_kwargs = {"action": "opens",
                "sql_insert_string" : sql_insert_str_opens,
                "sql_del_str": sql_del_str_opens},
    dag=dag
)


sql_del_str_bounces = """ DELETE FROM campaign_monitor.campaign_email_bounces """
sql_insert_str_bounces = """ INSERT INTO campaign_monitor.campaign_email_bounces (campaign_id, email_address, action_date ) VALUES %s """
insert_campaign_email_bounce_data = PythonOperator(
    task_id='insert_campaign_email_bounce_data',
    python_callable=insert_campaign_email_action_data_func,
    provide_context=True,
    op_kwargs = {"action": "bounces",
                "sql_insert_string" : sql_insert_str_bounces,
                "sql_del_str": sql_del_str_bounces},
    dag=dag
)

sql_del_str_unsubscribes = """ DELETE FROM campaign_monitor.campaign_email_unsubscribes """
sql_insert_str_unsubscribes = """ INSERT INTO campaign_monitor.campaign_email_unsubscribes (campaign_id, email_address, action_date ) VALUES %s """
insert_campaign_email_unsubscribe_data = PythonOperator(
    task_id='insert_campaign_email_unsubscribe_data',
    python_callable=insert_campaign_email_action_data_func,
    provide_context=True,
    op_kwargs = {"action": "unsubscribes",
                "sql_insert_string" : sql_insert_str_unsubscribes,
                "sql_del_str": sql_del_str_unsubscribes},
    dag=dag
)

sql_del_str_spams = """ DELETE FROM campaign_monitor.campaign_email_spam """
sql_insert_str_spams = """ INSERT INTO campaign_monitor.campaign_email_spams (campaign_id, email_address, action_date ) VALUES %s """
insert_campaign_email_spam_data = PythonOperator(
    task_id='insert_campaign_email_spam_data',
    python_callable=insert_campaign_email_action_data_func,
    provide_context=True,
    op_kwargs = {"action": "spam",
                "sql_insert_string" : sql_insert_str_spams,
                "sql_del_str": sql_del_str_spams},
    dag=dag
)


extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_click_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_open_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_bounce_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_unsubscribe_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_spam_data


