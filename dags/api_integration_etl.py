import logging
import pandas as pd
import requests

import sys
import numpy as np
from datetime import datetime, timedelta

from psycopg2.extras import execute_values

from airflow import DAG
from airflow import AirflowException

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import my_custom_file as cf

dr_obj =  cf.API_DataReader()

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
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Get Campaign Data and Email User Actions Data Functions
#
#########################################################

def get_campaign_action_data_func(**kwargs):


    ti = kwargs['ti']
    all_campaign_dict = ti.xcom_pull(task_ids=f'get_all_sent_campaign_list_data')

    df_all_sent_campaigns_list = pd.DataFrame(all_campaign_dict)

    print("df_all_sent_campaigns_list:", df_all_sent_campaigns_list)

    print("cfg_load_method", dr_obj.cfg_load_method)

    ###### Identify the requirement of load method ######
    # if Full, all the campaigns in the history will be downloaded
    # if Incremental, only active campaigns will be downloaded
    if (dr_obj.cfg_load_method == 'full'):
        campaigns_on_active_period = df_all_sent_campaigns_list
    elif (dr_obj.cfg_load_method == 'incremental'):
        campaigns_on_active_period = dr_obj.getFilteredActiveCampaign(df_all_sent_campaigns_list)

    print("campaigns_on_active_period", campaigns_on_active_period)
    # ###### Loop by each campaign
    for i in range(len(campaigns_on_active_period)) : 
        
        # Get the identifier of the campaign looping now
        campaign_startDate = campaigns_on_active_period.loc[i, 'SentDate']
        campaign_id = campaigns_on_active_period.loc[i, 'CampaignID'] 
        
        
        # Count the number of days between C-SentDate and End-Date
        number_of_days = dr_obj.cfg_cmp_active_period_length 
        
        ###### Loop by each action
        for action in dr_obj.campaign_email_action_list:
            
            ### Set the Query Start Date
            query_startDate = campaign_startDate
            if(dr_obj.cfg_load_method == 'incremental'):
                
                ## Set the load starting time according to the last loaded time recorded in the db-log
                query_startDate = dr_obj.set_query_start_date(dr_obj.final_campaign_action_data[action]['destination_table_name'])
                print("query_startDate", query_startDate)
                ## Overwriting number_of_days - if "incremental" to get the exact required count of loop
                number_of_days = dr_obj.cal_number_of_days_to_campaingnEndDay(campaign_startDate, dr_obj.cfg_cmp_active_period_length)
        
            ## Define the API End point  
            url_campaign_opens = f"https://api.createsend.com/api/v3.2/campaigns/{campaign_id}/{action}.json"
            # placeholder for each action's data
            df_campaign_email_action = pd.DataFrame()
            
            #### Loop by each day for one action for one campaign
            for x in range(number_of_days):
                # Increase the next date count in every round of loop
                queryDate = dr_obj.get_next_NDays(query_startDate, x)
                # Update the API param "date"
                dr_obj.params["date"] = queryDate
                
                # active the GET request
                response = requests.get(url_campaign_opens, headers={'Authorization': dr_obj.auth_header }, params = dr_obj.params) 
                response_campaign_action = response.json()

                # logging.info("response_campaign_action length: ", len(response_campaign_action["Results"]) )
                #print("length", len(response_campaign_action["Results"]))
                #print("action", action)
                
                # If data is empty , skip the loop  
                if(len(response_campaign_action["Results"]) == 0): continue

                # Convert API response in dict format to Dataframe format
                response_dataframe = pd.DataFrame.from_dict(response_campaign_action["Results"])
                
                # Add "campaignId" value to the column
                response_dataframe["campaignId"] = campaign_id
                # Select the desired columns only from the entire response to save into the DB
                response_dataframe = response_dataframe[dr_obj.desired_columns]
                # Add up the dataframe to the result placeholder
                df_campaign_email_action = df_campaign_email_action.append(response_dataframe, ignore_index=True, sort=False) 
            
            if(len(df_campaign_email_action.index) > 0):           
                campaign_email_action_dataset = df_campaign_email_action.drop_duplicates()
                dr_obj.final_campaign_action_data[action]["data"] = dr_obj.final_campaign_action_data[action]["data"].append(campaign_email_action_dataset, ignore_index=True, sort=False) 
                
    return dr_obj.final_campaign_action_data

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
            if (dr_obj.cfg_load_method == 'full'):
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
            log_values = [[dr_obj.start_proc_time.strftime('%Y-%m-%d'),
                dr_obj.cfg_load_method,
                dr_obj.cfg_start_date.strftime('%Y-%m-%d'),
                dr_obj.cfg_end_date.strftime('%Y-%m-%d'),
                dr_obj.start_proc_time,
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

    

#########################################################
#
#   DAG Operator Setup
#
#########################################################

extract_all_sent_campaign_task = PythonOperator(
    task_id=f'get_all_sent_campaign_list_data',
    python_callable=dr_obj.getAllSentCampaigns_func,
    provide_context=True,
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



#########################################################
#
#   DAG Operators Sequencing
#
#########################################################
# extract_all_sent_campaign_task >> extract_campaign_user_email_actions_data >>[insert_campaign_email_click_data,
#                                                                             insert_campaign_email_open_data, 
#                                                                             insert_campaign_email_bounce_data,
#                                                                             insert_campaign_email_unsubscribe_data,
#                                                                             insert_campaign_email_spam_data]

extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_click_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_open_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_bounce_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_unsubscribe_data
extract_all_sent_campaign_task  >> extract_campaign_user_email_actions_data >> insert_campaign_email_spam_data