import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


d_args = {
    'owner': 'vahid',
    'retry_delay': timedelta(minutes=5),
}

report_dag =  DAG(
    dag_id = 'Daily_Report',
    tags = ['vahid'],
    default_args = d_args,
    schedule_interval = '@daily',
    start_date = days_ago(1),
)




def run_this_func():
    print('I am coming first')


def run_this_func_too():
    from minio import Minio
    import pandas as pd
    from IPython.display import display
    from io import BytesIO
    client = Minio("play.min.io", access_key="Q3AM3UQ867SPQQA43P2F", secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", secure=True)
    #------------------------------------Load csv file into DataFrame------------------------------------
    def preparefiles():
        #---DimDate
        obj_DimDate = client.get_object("grocery","DimDate.csv")
        df_DimDate = pd.read_csv(obj_DimDate)
        df_DimDate_rename = df_DimDate.rename(columns={  'GreDAte_str'  : 'Date' }  )
        #--DimVehicle
        obj_DimVehicle = client.get_object("grocery","DimVehicle.csv",)
        df_DimVehicle = pd.read_csv(obj_DimVehicle)
        #--DimBusinessProduct
        obj_DimBusinessProduct = client.get_object("grocery","DimBusinessProduct.csv",)
        df_DimBusinessProduct = pd.read_csv(obj_DimBusinessProduct)
        #--FactLead
        obj_FactLead = client.get_object("grocery","FactLead.csv",)
        df_FactLead = pd.read_csv(obj_FactLead)
        df_FactLead_rename = df_FactLead.rename(columns={  'BusinessProducts'  : 'BusinessProductID'  , 'Vehicle' : 'VehicleID'}  )
    #-----------------------------------Create Flattened table--------------------------------
        df_FactWithBusinessProduct=pd.merge(df_FactLead_rename,df_DimBusinessProduct, on='BusinessProductID')
        df_FactWithDate = pd.merge (df_FactWithBusinessProduct , df_DimDate_rename , on='Date')
        df_flattened = pd.merge ( df_FactWithDate , df_DimVehicle , on='VehicleID')
        #df_flattened = df_flattened.CommisionPrice.replace('',np.nan,regex = True)
        df_flattened_Clean = df_flattened [["LeadID" , "PerDate", "perDate_Full" , "PerSal" , "PerMah", "PerMahName", "Per_Day" ,"GreDate" , "GreYear" , "ProductGroup" , "AdvisorTeam" , "CarTip" , "CarModel" , "Superviser" , "Adviser" , "ProductManager" , "SalePersonTeam" , "CarMaker" , "OwnerName" , "VehicleStatus" , "VehicleName" , "VehicleDiscount" , "BusinessProductName" , "NewContacts","RepeatitiveContacts","StatusOpenCount","StatusDisqualifiedCount","First Contact In","Qualify In"	,"AutoExpoId"	,"Date_AutoExpo","SaleChannels","CancelCount","CommisionPrice","DealPrice","SoldCount"]]
        print(df_flattened['AutoExpoId'])
    #-----------------------------------Put Flattend table on Minio--------------------------
        csv_bytes = df_flattened_Clean.to_csv().encode('utf-8-sig')
        csv_buffer = BytesIO(csv_bytes)
        client.put_object('grocery',
                           'Flattened.csv',
                            data=csv_buffer,
                            length=len(csv_bytes),
                            content_type='application/csv')
    preparefiles()



run_this_task = PythonOperator(
    task_id='run_this_first',
    python_callable = run_this_func,
    dag = report_dag
)

run_this_task_too = PythonOperator(
    task_id='run_this_last',
    python_callable = run_this_func_too,
    dag = report_dag
)

run_this_task >> run_this_task_too
