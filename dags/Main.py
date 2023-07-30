import connection
import pandas as pd
from IPython.display import display
from io import BytesIO
import numpy as np

#------------------------------------Load csv file into DataFrame------------------------------------
def prepare_FlatTable():
    #---DimDate
    obj_DimDate = connection.client.get_object("grocery","DimDate.csv")
    df_DimDate = pd.read_csv(obj_DimDate)
    df_DimDate_rename = df_DimDate.rename(columns={  'GreDAte_str'  : 'Date' }  )



    #--DimVehicle
    obj_DimVehicle = connection.client.get_object("grocery","DimVehicle.csv",)
    df_DimVehicle = pd.read_csv(obj_DimVehicle)

    #--DimBusinessProduct
    obj_DimBusinessProduct = connection.client.get_object("grocery","DimBusinessProduct.csv",)
    df_DimBusinessProduct = pd.read_csv(obj_DimBusinessProduct)



    #--FactLead
    obj_FactLead = connection.client.get_object("grocery","FactLead.csv",)
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

    connection.client.put_object('grocery',
                       'Flattened.csv',
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')



