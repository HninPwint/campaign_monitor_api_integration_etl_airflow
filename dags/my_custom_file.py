import pandas as pd
import requests
from base64 import b64encode
import yaml as yml

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook

class API_DataReader:

    def __init__(self):

        ### Initial 
        ### THis is to update accordingly    
        #apikey = "Get_API_Key_From_Campaign_Monitor_Interace"
        #self.clientId = "Get_Client_ID_From_Campaign_Monitor_Interace"

        apikey = "fz/ocDZn9rHm9ZAupGsDNjoBZGSnHO+8vmU/xWiPPvvFzPE0STJnTbnvu6/FvyOpUhp/TBu0txZ4xZYz6J8iyxkUDkBowVTiDflAvH0qdi5NzWSW+iq8L7j77/Qg77/6Wkj1JvxGkBTOtFOpV9saGw=="
        self.clientId = "33604187fc197c87eb8d2f553935ef8e"

        self.page = 1
        self.pagesize = 1000

        # Encode the credentials with base64
        username = apikey
        password = ''
        encoded_credentials = b64encode(bytes(f'{username}:{password}',encoding='ascii')).decode('ascii')

        # Use Basic Authorization
        self.auth_header = f'Basic {encoded_credentials}'
        print(f'Auth header: {self.auth_header}')

        # Open the config file
        with open(r'/opt/airflow/dags/dbconfig.yaml') as file:
            self.config = yml.full_load(file)

        self.campaign_email_action_list = ["opens", "clicks", "unsubscribes", "bounces", "spam"]

        # Base API parameters
        self.params = {"page": self.page, "pagesize": self.pagesize, "orderfield": "email", "orderdirection": "asc" }

        ## The list of columns to select from the original API response. Only these columns will be stored into the Database
        self.desired_columns = ["campaignId", "EmailAddress", "Date"]
        #desired_columns = ["campaign_id", "email_address", "action_date"]

        ## Place holder to store the downloaded data
        self.final_campaign_action_data = {"opens": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_opens"},
                                    "clicks":{"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_clicks"},
                                    "unsubscribes": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_unsubscribes"},
                                    "bounces": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_bounces"},
                                    "spam": {"data": pd.DataFrame(), "destination_table_name": "campaign_monitor.campaign_email_spams"} }

            ####### Initialise the config related variable ######
        # start time of processing
        self.start_proc_time = datetime.now()

        # Data loading method - "full" for the first time and "incremental" since second run
        # Check loading configuration #METHOD: full/incremental
        self.cfg_load_method = self.config['loading']['method']

        #START_DATE_FULL_LOAD: starting date of data to be loaded (if method is full-load)
        self.cfg_start_date = self.config['loading']['start_date_full_load']

        #DAY_UPDATE_LAG: number of days of data lag
        self.cfg_update_lag = self.config['loading']['day_update_lag']

        # Calculate end date (the last latest day to extract/download data according to config 'cfg_update_lag')
        self.cfg_end_date = datetime.now() - relativedelta(days = self.cfg_update_lag)

        # number of days to consider a campaign is active
        self.cfg_cmp_active_period_length = self.config['loading']['campaign_active_period_length']

        # number of months to consider a campaign is active
        self.default_full_load_month = self.config['loading']['default_full_load_month']

        # Time to mark the end of processing
        self.end_proc_time = 0

    ### Get the sent campaign


    def set_query_start_date(self, tableName):
        query_startDate = self.get_last_success_load_time(tableName)
        return query_startDate

    def get_last_success_load_time(self, table):
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

    def getFilteredActiveCampaign(self,df_campaign_list): 
        '''
        The function filters the campaigns which are still within n(for instance: 120) days since the email sent date.
        Basically, get all the still-active campaigns under monitoring period set by config
        '''
        # Get n() days ago from now
        last_active_date = self.get_datetime_Ndays_ago(self.cfg_cmp_active_period_length)
        
        # The campaign with SentDate which are greater than xxx, means within the monitoring period 
        # Get the index of campaigns running for less than n days 
        all_active_campaign = df_campaign_list['SentDate'] > last_active_date 
        
        # Filter out only the active campaigns
        campaigns_on_active_period = df_campaign_list.loc[all_active_campaign]
        
        return campaigns_on_active_period

    def get_datetime_Ndays_ago(self, n):
        N_days_ago_datetime = datetime.now() - timedelta(days=n)
        return N_days_ago_datetime.strftime("%Y-%m-%d %H:%M:%S")

    def get_next_NDays(self, dateFrom, n):
        ### Convert to datetime type to do date addition
        begindate = datetime.strptime(dateFrom, "%Y-%m-%d %H:%M:%S") 
        
        next_N_days_datetime = begindate + timedelta(days=n)
        ## return the string format of next N date
        return next_N_days_datetime.strftime("%Y-%m-%d %H:%M:%S")

    def cal_number_of_days_to_campaingnEndDay(self, dateFrom, n):

        # Get the campaign end date    
        datetime_object = datetime.strptime(dateFrom, '%Y-%m-%d %H:%M:%S')
        next_N_days_datetime = datetime_object + timedelta(days=n)
        
        # If the campaign end date is already passed
        if (datetime.now() > next_N_days_datetime):
            end_day = next_N_days_datetime
            
        # If the campaign end date is in future, it should be n(2) days ago
        elif ( datetime.now() < next_N_days_datetime):
            end_day = self.get_datetime_Ndays_ago(self.cfg_update_lag)
            end_day = datetime.strptime(end_day, '%Y-%m-%d %H:%M:%S')

        # Calculate the difference between two dates
        number_of_days_required = end_day - datetime_object 
        return(number_of_days_required.days)
    
    def getAllSentCampaigns_func(self):
        print(" self.auth_header",  self.auth_header)
        print("self.clientId", self.clientId)
        campaign_detail_url = f'https://api.createsend.com/api/v3.2/clients/{self.clientId}/campaigns.json'

        response = requests.get(campaign_detail_url, headers={'Authorization': self.auth_header } ) 
        response_campaign_list = response.json()
        df_campaign_list = pd.DataFrame.from_dict(response_campaign_list)

        print("length", df_campaign_list)
        return df_campaign_list