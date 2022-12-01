from dotenv import load_dotenv
load_dotenv()
from datetime import date
from datetime import datetime
import json
import os
import numpy as np
import pandas as pd
import pyodbc
import json
import csv
import requests
import json 

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    print(response)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response     

def markWrikeTaskComplete (taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/"
    querystring = {
        'status':'Completed'
        }     
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    response = requests.request("PUT", url, headers=headers, params=querystring)
    return response       

def get_google_product_type (row):
    if row['google_product_category'] is None or row['google_product_category']==np.nan:
        return np.nan     
    else:
        return row['google_product_category'].split(' >', 1)[0]

def yesno_to_truefalse (row, column_name):
    try:
        if row[column_name] == 'Y':
            return True
        elif row[column_name] == 'N':
            return False
    finally:
        return np.nan   

def make_json_association_data_nest(row, column_name):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        row[column_name] = np.nan
    elif str(row[column_name]) == '':
        row[column_name] = {"products":[]}
    elif type(row[column_name]) != dict:
        if not isinstance(row[column_name], str):
            d = str(row[column_name]).split(",")
        else:
            d = row[column_name].split(",")
        row[column_name]  = {"products":d}       
    return row

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if isinstance(row[column_name], bool):
            d = row[column_name]
        elif not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
        else:
            d = row[column_name].encode().decode()
        if unit is not None and currency is None:
            if row[column_name] == '':
                row[column_name] = np.nan
                return row
            else:
                d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row    

if __name__ == '__main__':

    pd.options.display.max_colwidth = 9999

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        #sys.path.append("../../Utilities")
        sys.path.append("..")
        from akeneo_api_client.client import Client

    #Stored in .env
    AKENEO_CLIENT_ID = os.environ.get("TO_SAGE_AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("TO_SAGE_AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("TO_SAGE_AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("TO_SAGE_AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("TO_SAGE_AKENEO_BASE_URL")

    #Establish akeneo API client
    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)    

    attributeCols = [
        'VendorAlias',
        'Catalog',
        'UPC',
        'GTIN',
        'DisplayName',
        'VendorPriceDate',
        'COO',
        'Brand',
        'ProductType',
        'Replacement',
        'freeship_excluded',
        'SageCreatedDate',                   
        'OnAmazonVendor',
        'OnGlobal',
        'OnWalmart',
        'AmazonSKU',
        'AmazonASIN',
        'AmazonVendorDiscount',   
        'DefaultWarehouse',
        'PrimaryVendorNo',
        'TempTitle150',
        'From_Sage_Sync_Date'
    ]    
    #To handle Akeneo's 'units'
    unitCols = [
        'ShippingWeight'                                            
    ]    
    #To handle Akeneo's currency types
    currencyCols = [
        'Cost',
        'SalePrice',
        'MSRP',
        'MAP'
    ]     
    #Deprecated
    associationCols = [                   
    ]            
    enabledCols = [
        'enabled'                       
    ]
    idCols = [
        'identifier'                       
    ]       
    #List of everyhing that needs a pandas to JSON conversion
    jsonCols = idCols + enabledCols + attributeCols + currencyCols + unitCols + associationCols
    #List of 
    akeneoDataCols = [
        'Category2',
        'Category4',
        'DateCreated',
        'DefaultWarehouseCode',
        'InactiveItem',
        'PrimaryVendorNo',
        'ProductLine',
        'ProductType',
        'ShipWeight',
        'StandardUnitCost',
        'StandardUnitPrice',
        'SuggestedRetailPrice',
        'TotalQuantityOnHand',
        'UDF_AMAZ002_MAX',
        'UDF_AMAZ002_MAX_PM',
        'UDF_AMAZ002_MIN',
        'UDF_AMAZ002_MIN_PM',
        'UDF_AMAZON_ASIN',
        'UDF_AMAZON_SKU',
        'UDF_AMAZON_VENDOR_DISCOUNT',
        'UDF_AMAZON_VENDOR_PRICE',
        'UDF_AMAZON_VENDOR_PRICE_DATE',
        'UDF_CATALOG_NO',
        'UDF_COUNTRY_OF_ORIGIN_TEMP',
        'UDF_DISCONTINUED_STATUS',
        'UDF_DRP_SHP_ONLY',
        'UDF_EBAY_PM',
        'UDF_EBAY_PRICE',
        'UDF_ECCN',
        'UDF_GLOBAL_PM',
        'UDF_GLOBAL_PRICE',
        'UDF_GSA_GM',
        'UDF_GSA_PRICE',
        'UDF_GSA_PRICE_DATE',
        'UDF_GSA_TEMP_PRICE',
        'UDF_GSA_TEMP_PRICE_DATE',
        'UDF_GTIN14',
        'UDF_ISAMAZ002',
        'UDF_ISAMAZ009',
        'UDF_ISEBAY',
        'UDF_ISGLOBAL',
        'UDF_ISWALMART',
        'UDF_LOWEST_PRICE',
        'UDF_MAP_PRICE',
        'UDF_ON_CLEARANCE',
        'UDF_PRODUCT_NAME_150',
        'UDF_PACK_QUANTITY',
        'UDF_REPLACEMENT_ITEM',
        'UDF_RETAIL_GM',
        'UDF_REVIEW_REQUIRED',
        'UDF_RFQ',
        'UDF_SCHEDULE_B_NUMBER',
        'UDF_SEARS_PM',
        'UDF_SEARS_PRICE',
        'UDF_SHIPPING_EXCLUSION',
        'UDF_SPECIALORDER',
        'UDF_STD_GM',
        'UDF_UPC',
        'UDF_VENDOR_PRICE_DATE',
        'UDF_WALMART_PM',
        'UDF_WALMART_PRICE',
        'UDF_WEB_DISPLAY_MODEL_NUMBER',
        'UDF_CALL',
        'VendorAliasItemNo'
    ]

    sage_conn_str = (
        os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 
    )        

    #Establish sage connection
    sage_cnxn = pyodbc.connect(sage_conn_str, autocommit=True)
    #SQL Sage 000 warehouse data into dataframe
    SageSQLquery = """
        SELECT CI_Item.ItemCode,CI_Item.Category2,CI_Item.Category4,CI_Item.DateCreated,CI_Item.DefaultWarehouseCode,CI_Item.InactiveItem,CI_Item.PrimaryVendorNo,CI_Item.ProductLine,
            CI_Item.ProductType,CI_Item.ShipWeight,CI_Item.StandardUnitCost,CI_Item.StandardUnitPrice,CI_Item.SuggestedRetailPrice,CI_Item.TotalQuantityOnHand,CI_Item.UDF_AMAZ002_MAX,
            CI_Item.UDF_AMAZ002_MAX_PM,CI_Item.UDF_AMAZ002_MIN,CI_Item.UDF_AMAZ002_MIN_PM,CI_Item.UDF_AMAZON_ASIN,CI_Item.UDF_AMAZON_SKU,CI_Item.UDF_AMAZON_VENDOR_DISCOUNT,
            CI_Item.UDF_AMAZON_VENDOR_PRICE,CI_Item.UDF_AMAZON_VENDOR_PRICE_DATE,CI_Item.UDF_CATALOG_NO,CI_Item.UDF_COUNTRY_OF_ORIGIN_TEMP,CI_Item.UDF_DISCONTINUED_STATUS,
            CI_Item.UDF_DRP_SHP_ONLY,CI_Item.UDF_EBAY_PM,CI_Item.UDF_EBAY_PRICE,CI_Item.UDF_ECCN,CI_Item.UDF_GLOBAL_PM,CI_Item.UDF_GLOBAL_PRICE,CI_Item.UDF_GSA_GM,CI_Item.UDF_GSA_PRICE,
            CI_Item.UDF_GSA_PRICE_DATE,CI_Item.UDF_GSA_TEMP_PRICE,CI_Item.UDF_GSA_TEMP_PRICE_DATE,CI_Item.UDF_GTIN14,CI_Item.UDF_ISAMAZ002,CI_Item.UDF_ISAMAZ009,CI_Item.UDF_ISEBAY,
            CI_Item.UDF_ISGLOBAL,CI_Item.UDF_ISWALMART,CI_Item.UDF_LOWEST_PRICE,CI_Item.UDF_MAP_PRICE,CI_Item.UDF_ON_CLEARANCE,CI_Item.UDF_PRODUCT_NAME_150,
            CI_Item.UDF_PACK_QUANTITY,CI_Item.UDF_REPLACEMENT_ITEM,CI_Item.UDF_RETAIL_GM,CI_Item.UDF_REVIEW_REQUIRED,CI_Item.UDF_RFQ,CI_Item.UDF_SCHEDULE_B_NUMBER,CI_Item.UDF_SEARS_PM,
            CI_Item.UDF_SEARS_PRICE,CI_Item.UDF_SHIPPING_EXCLUSION,CI_Item.UDF_SPECIALORDER,CI_Item.UDF_STD_GM,CI_Item.UDF_UPC,CI_Item.UDF_VENDOR_PRICE_DATE,CI_Item.UDF_WALMART_PM,
            CI_Item.UDF_WALMART_PRICE,CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER,CI_Item.UDF_CALL,CI_Item.UDF_PRICE_STATUS_CODE,CI_Item.UDF_PRICE_STATUS_DATE,
            IM_ItemVendor.VendorAliasItemNo
        FROM CI_Item CI_Item, IM_ItemVendor IM_ItemVendor
        WHERE CI_Item.ItemCode = IM_ItemVendor.ItemCode AND CI_Item.PrimaryVendorNo = IM_ItemVendor.VendorNo AND CI_Item.DefaultWarehouseCode='000'"""
                                    
    #Execute SQL
    print('Retrieving Sage Warehouse 000 data')
    todayakeneoDF = pd.read_sql(SageSQLquery,sage_cnxn)
    todayakeneoDF = todayakeneoDF.set_index('ItemCode')

    #SQL Sage relavent non-000 warehouse data into dataframe
    SageSQLquery = """
        SELECT CI_Item.ItemCode,CI_Item.Category2,CI_Item.Category4,CI_Item.DateCreated,CI_Item.DefaultWarehouseCode,CI_Item.InactiveItem,CI_Item.PrimaryVendorNo,CI_Item.ProductLine,
            CI_Item.ProductType,CI_Item.ShipWeight,CI_Item.StandardUnitCost,CI_Item.StandardUnitPrice,CI_Item.SuggestedRetailPrice,CI_Item.TotalQuantityOnHand,CI_Item.UDF_AMAZ002_MAX,
            CI_Item.UDF_AMAZ002_MAX_PM,CI_Item.UDF_AMAZ002_MIN,CI_Item.UDF_AMAZ002_MIN_PM,CI_Item.UDF_AMAZON_ASIN,CI_Item.UDF_AMAZON_SKU,CI_Item.UDF_AMAZON_VENDOR_DISCOUNT,
            CI_Item.UDF_AMAZON_VENDOR_PRICE,CI_Item.UDF_AMAZON_VENDOR_PRICE_DATE,CI_Item.UDF_CATALOG_NO,CI_Item.UDF_COUNTRY_OF_ORIGIN_TEMP,CI_Item.UDF_DISCONTINUED_STATUS,
            CI_Item.UDF_DRP_SHP_ONLY,CI_Item.UDF_EBAY_PM,CI_Item.UDF_EBAY_PRICE,CI_Item.UDF_ECCN,CI_Item.UDF_GLOBAL_PM,CI_Item.UDF_GLOBAL_PRICE,CI_Item.UDF_GSA_GM,CI_Item.UDF_GSA_PRICE,
            CI_Item.UDF_GSA_PRICE_DATE,CI_Item.UDF_GSA_TEMP_PRICE,CI_Item.UDF_GSA_TEMP_PRICE_DATE,CI_Item.UDF_GTIN14,CI_Item.UDF_ISAMAZ002,CI_Item.UDF_ISAMAZ009,CI_Item.UDF_ISEBAY,
            CI_Item.UDF_ISGLOBAL,CI_Item.UDF_ISWALMART,CI_Item.UDF_LOWEST_PRICE,CI_Item.UDF_MAP_PRICE,CI_Item.UDF_ON_CLEARANCE,CI_Item.UDF_PRODUCT_NAME_150,
            CI_Item.UDF_PACK_QUANTITY,CI_Item.UDF_REPLACEMENT_ITEM,CI_Item.UDF_RETAIL_GM,CI_Item.UDF_REVIEW_REQUIRED,CI_Item.UDF_RFQ,CI_Item.UDF_SCHEDULE_B_NUMBER,CI_Item.UDF_SEARS_PM,
            CI_Item.UDF_SEARS_PRICE,CI_Item.UDF_SHIPPING_EXCLUSION,CI_Item.UDF_SPECIALORDER,CI_Item.UDF_STD_GM,CI_Item.UDF_UPC,CI_Item.UDF_VENDOR_PRICE_DATE,CI_Item.UDF_WALMART_PM,
            CI_Item.UDF_WALMART_PRICE,CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER,CI_Item.UDF_CALL,CI_Item.UDF_PRICE_STATUS_CODE,CI_Item.UDF_PRICE_STATUS_DATE
        FROM CI_Item CI_Item
        WHERE CI_Item.DefaultWarehouseCode <> '000' AND CI_Item.DefaultWarehouseCode <> 'CAL' AND CI_Item.DefaultWarehouseCode <> 'REP' """

    #Execute SQL
    print('Retrieving Sage non-000 non-CAL Warehouse data')

    #merge 000 and 500
    todayakeneoDF = pd.concat([todayakeneoDF,pd.read_sql(SageSQLquery,sage_cnxn).set_index('ItemCode')],sort=False)

    #Keep 000 item over non-000
    todayakeneoDF = todayakeneoDF.reset_index().drop_duplicates(subset='ItemCode',keep='first').set_index('ItemCode').sort_index()

    #Grab only IM Warehouse data
    SageSQLquery = """
        SELECT IM_ItemWarehouse.ItemCode, IM_ItemWarehouse.WarehouseCode, IM_ItemWarehouse.QuantityOnHand, IM_ItemWarehouse.ReorderPointQty,
            IM_ItemWarehouse.UDF_REORDER_POINT_NOTES, IM_ItemWarehouse.UDF_REORDER_POINT_DATE_UPDATED
        FROM IM_ItemWarehouse IM_ItemWarehouse
        WHERE (((IM_ItemWarehouse.UDF_REORDER_POINT_NOTES) Is Not Null)) OR 
            (((IM_ItemWarehouse.ReorderPointQty)>0)) OR 
            (((IM_ItemWarehouse.UDF_REORDER_POINT_DATE_UPDATED) Is Not Null)) OR 
            (((IM_ItemWarehouse.QuantityOnHand)>0))
        """
    #Execute SQL
    print('Retrieving Sage Warehouse Data')
    warehouseDF = pd.read_sql(SageSQLquery,sage_cnxn)

    #Pivot data to get warehouse versions of the columns
    print('pivoting warehouse data')
    warehouseDF = warehouseDF.query("WarehouseCode == '000' | WarehouseCode == '500' | WarehouseCode == 'BVA' | WarehouseCode == 'BVU'")
    warehouseDF = warehouseDF.pivot(index='ItemCode', columns='WarehouseCode', values=['QuantityOnHand', 'ReorderPointQty', 'UDF_REORDER_POINT_NOTES', 'UDF_REORDER_POINT_DATE_UPDATED']).reset_index()
    warehouseDF.columns = warehouseDF.columns.map('-'.join)
    warehouseDF = warehouseDF.rename({'ItemCode-':'ItemCode'}, axis=1)
    warehouseDF = warehouseDF.dropna(how='all', axis='columns')

    #Merge with previously pull CI.Item Data
    print('Merging warehouse and item data')
    todayakeneoDF = todayakeneoDF.merge(warehouseDF, how='left', left_on='ItemCode', right_on='ItemCode').set_index('ItemCode').sort_index()

    #Shipping Effect Estimates
    todayakeneoDF['ShipWeight'] = pd.to_numeric(todayakeneoDF['ShipWeight'])                
    todayakeneoDF['ShippingEffect'] = todayakeneoDF['ShipWeight'] * -2 / 3
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 100, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -3 / 4
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 45, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -4 / 5
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 25, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -1
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 20, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -7 / 6
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 15, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -7 / 5
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 10, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -2
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 7, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -2.5
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 5, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -2.8
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 4, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -4
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 3, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -5
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 2, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -7
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] < 1.5, 'ShippingEffect'] = todayakeneoDF['ShipWeight'] * -9
    todayakeneoDF.loc[todayakeneoDF['ShipWeight'] <= 1, 'ShippingEffect'] = -9
    todayakeneoDF.loc[todayakeneoDF['StandardUnitPrice'] < 75, 'ShippingEffect'] = 3       

    #Uncomment this and run when wanting to add columns to a new version   
    # print('making new last akeneo!')
    # todayakeneoDF.to_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\akeneolyzer\LastAkeneoLyzer')    
    # exit()    

    #Data Dumps for Backups
    print("Dumping to Archive")    
    todayakeneoDF.replace(r'\n',' ', regex=True) .to_csv(r'\\FOT00WEB\Alt Team\Andrew\AndrewsItemFiles\Archive' + '\\' + datetime.today().strftime('%Y-%m-%d') + " SageDump.csv", header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL)
    todayakeneoDF.replace(r'\n',' ', regex=True) .to_csv(r'\\FOT00WEB\Alt Team\Andrew\AndrewsItemFiles\TodaysSageDump.csv', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL)

    #standardizing data before comparing... (Some data we dump [above] the rest is synced after this filter)
    columnstouse = list(todayakeneoDF)
    columnstouse = akeneoDataCols

    #Pulling last run data to compare to
    print("Pulling last run's data")
    lastakeneoDF = pd.read_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\akeneolyzer\LastAkeneoLyzer')
    lastakeneoDF.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\AndrewsItemFiles\\lastakeneoDF2.csv', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL)

    #Check to see if the data frames are identical...otherwise...not much to sync :D
    if not todayakeneoDF.reindex(columns = columnstouse).sort_index().equals(lastakeneoDF.reindex(columns = columnstouse).sort_index()):            
        
        #We need to make identically indexed dataframes to do a binary compare...so we need to remove any rows that only exist in one of two dataframes
        #So we stack the data frames on Top of each other and drop everything that's a duplicate
        unique_akeneoDF = pd.concat([todayakeneoDF.reindex(columns = columnstouse).sort_index(),lastakeneoDF.reindex(columns = columnstouse).sort_index()],sort=False).reset_index()
        unique_akeneoDF = unique_akeneoDF.drop_duplicates(subset='ItemCode',keep=False).set_index('ItemCode')

        #This checks to see if we have brand new items...or somehow ... someone... deleted items in Sage
        if unique_akeneoDF.shape[0] > 0:
            print(unique_akeneoDF)


            #just making sure no weird columns snuck in... probably safe to remove
            unique_akeneoDF = unique_akeneoDF.filter(columnstouse)

            #Unique items in today's extract are obviously new
            new_items_df = unique_akeneoDF[unique_akeneoDF.isin(todayakeneoDF)].dropna(how='all')
            #Unique items not today's extract (obviously in last data pull) must have been removed from sage
            del_items_df = unique_akeneoDF[~unique_akeneoDF.isin(todayakeneoDF)].dropna(how='all')
            del_items_df['InactiveItem'] = 'Y'

            #We need to remove the unqiue items from the data frames before we can do a binary compare (have to have to same indices...both rows and columns!)
            #Removing the new stuff from most recent data
            mask = ~todayakeneoDF.index.isin(unique_akeneoDF.index)
            todayakeneoDF_less_new = todayakeneoDF.loc[mask]
            #Removing deleted stuff from last akeneo
            mask = ~lastakeneoDF.index.isin(unique_akeneoDF.index)
            lastakeneoDF_less_dead = lastakeneoDF.loc[mask] 

            #I can finally do the binary compare that the columns and rows are identical
            #This creates the 'mask' that shows what data hase changed
            where_mask = (todayakeneoDF_less_new != lastakeneoDF_less_dead)
            #Filter out data that hasn't changed
            akeneoDF = todayakeneoDF_less_new.filter(columnstouse).where(cond=where_mask, other=np.nan)
            akeneoDF = akeneoDF.filter(columnstouse)
            #Drop any rows or columns that have no changes.....nothing to sync :D
            akeneoDF = akeneoDF.dropna(how='all')
            #Stack the new and deleted items back on our 'Only Changes' dataframe....The new and deleted dataframes will do a full data sync
            akeneoDF = pd.concat([akeneoDF,new_items_df,del_items_df],sort=False)     

        else:
            #Probably could re-write the the if to not include this else...but basically same as above just with dataframes that had no new items or deleted in sage
            where_mask = (todayakeneoDF.reindex(columns = columnstouse).sort_index() != lastakeneoDF.reindex(columns = columnstouse).sort_index())
            akeneoDF = todayakeneoDF.filter(columnstouse).where(cond=where_mask, other=np.nan)        
            akeneoDF = akeneoDF.filter(columnstouse)
            akeneoDF = akeneoDF.dropna(how='all')

        #We end up with akeneoDF...which is basically all the data that has changed

        #Quick hack to have a toggle in sage to do full syncs back to akeneo
        datasnycdf = todayakeneoDF.query("Category4 == 'SD'")
        akeneoDF.update(datasnycdf)

        #Reindexing now that we have all the data that we want to sync
        akeneoDF = akeneoDF.reindex(columns = columnstouse).sort_index()
        akeneoDF = akeneoDF.dropna(axis=0, how='all')
        
        #Enabled lives outside of akeneo's JSON 'values'.... we can't send null values...and I didn't want to program a JSON complier that could account for this (probably could be done...its a pain tho)
        #So this just fills them all in ...wether they changed or not (but just for the items we are sending data for)
        akeneoDF.loc[:, ['InactiveItem']] = todayakeneoDF[['InactiveItem']]

        #Is there data to sync?
        if akeneoDF.shape[0] > 0:

            print(akeneoDF)
            print('there are things to sync...')
            print(akeneoDF.shape[0])
            print("...of em")
            
            #not sure I need this...but keeping it for now
            akeneoDF = akeneoDF.reset_index()

            #Sage to Akeneo Mapping
            akeneoColMap = {'InactiveItem': 'enabled',
                            'ItemCode':'identifier', 
                            'VendorAliasItemNo': 'VendorAlias',
                            'UDF_CATALOG_NO': 'Catalog',
                            'UDF_WEB_DISPLAY_MODEL_NUMBER': 'DisplayName',
                            'UDF_UPC': 'UPC',
                            'UDF_GTIN14': 'GTIN',
                            'StandardUnitCost': 'Cost',
                            'StandardUnitPrice': 'SalePrice',
                            'SuggestedRetailPrice': 'MSRP',
                            'ProductLine': 'Brand',                        
                            'UDF_MAP_PRICE': 'MAP',
                            'UDF_COUNTRY_OF_ORIGIN_TEMP': 'COO',
                            'UDF_REPLACEMENT_ITEM': 'Replacement',
                            'DateCreated': 'SageCreatedDate',
                            'UDF_ISAMAZ009': 'OnAmazonVendor',
                            'UDF_ISAMAZ002': 'OnAmazonSeller',
                            'UDF_ON_CLEARANCE': 'Clearance',
                            'UDF_AMAZON_ASIN': 'AmazonASIN',
                            'UDF_AMAZON_SKU': 'AmazonSKU',
                            'UDF_ISEBAY': 'OnEbay',
                            'UDF_VENDOR_PRICE_DATE': 'VendorPriceDate',
                            'UDF_SHIPPING_EXCLUSION': 'freeship_excluded',
                            'UDF_AMAZON_VENDOR_DISCOUNT': 'AmazonVendorDiscount',
                            'UDF_AMAZON_VENDOR_PRICE': 'AmazonVendorPrice',
                            'UDF_AMAZON_VENDOR_PRICE_DATE': 'AmazonVendorPriceDate',
                            'UDF_DRP_SHP_ONLY': 'DropShipOnly',
                            'UDF_ISGLOBAL': 'OnGlobal',
                            'UDF_ISWALMART': 'OnWalmart',
                            'UDF_GSA_PRICE_DATE': 'GsaPriceDate',
                            'UDF_GSA_TEMP_PRICE': 'GsaTempPrice',
                            'UDF_GSA_TEMP_PRICE_DATE': 'GsaTempPriceDate',
                            'UDF_REVIEW_REQUIRED': 'ReviewReq',
                            'ShipWeight': 'ShippingWeight',
                            'DefaultWarehouseCode': 'DefaultWarehouse',
                            'UDF_PRODUCT_NAME_150':'TempTitle150',
                            'UDF_SPECIALORDER':'SpecialOrder'
                            }       
            akeneoDF = akeneoDF.rename(columns=akeneoColMap)

            #Converting these to the boolean inverse... as we just renamed 'InactiveItem': 'enabled'
            akeneoDF.loc[akeneoDF['enabled'] == 'Y','enabled'] = False
            akeneoDF.loc[akeneoDF['enabled'] == 'N','enabled'] = True

            #Boolean Conversions (probably cleaner as a for loop)
            akeneoDF.loc[akeneoDF['freeship_excluded'] == 'Y','freeship_excluded'] = True
            akeneoDF.loc[akeneoDF['Clearance'] == 'Y','Clearance'] = True
            akeneoDF.loc[akeneoDF['DropShipOnly'] == 'Y','DropShipOnly'] = True
            akeneoDF.loc[akeneoDF['ReviewReq'] == 'Y','ReviewReq'] = True
            akeneoDF.loc[akeneoDF['SpecialOrder'] == 'Y','SpecialOrder'] = True
            akeneoDF.loc[akeneoDF['OnAmazonSeller'] == 'Y','OnAmazonSeller'] = True
            akeneoDF.loc[akeneoDF['OnAmazonVendor'] == 'Y','OnAmazonVendor'] = True
            #akeneoDF.loc[akeneoDF['OnNewEgg'] == 'Y','OnNewEgg'] = True
            akeneoDF.loc[akeneoDF['OnGlobal'] == 'Y','OnGlobal'] = True
            #akeneoDF.loc[akeneoDF['OnJet'] == 'Y','OnJet'] = True
            akeneoDF.loc[akeneoDF['OnWalmart'] == 'Y','OnWalmart'] = True
            akeneoDF.loc[akeneoDF['OnEbay'] == 'Y','OnEbay'] = True

            akeneoDF.loc[akeneoDF['freeship_excluded'] == 'N','freeship_excluded'] = False
            akeneoDF.loc[akeneoDF['Clearance'] == 'N','Clearance'] = False
            akeneoDF.loc[akeneoDF['DropShipOnly'] == 'N','DropShipOnly'] = False
            akeneoDF.loc[akeneoDF['ReviewReq'] == 'N','ReviewReq'] = False
            akeneoDF.loc[akeneoDF['SpecialOrder'] == 'N','SpecialOrder'] = False
            akeneoDF.loc[akeneoDF['OnAmazonSeller'] == 'N','OnAmazonSeller'] = False
            akeneoDF.loc[akeneoDF['OnAmazonVendor'] == 'N','OnAmazonVendor'] = False
            #akeneoDF.loc[akeneoDF['OnNewEgg'] == 'N','OnNewEgg'] = False
            akeneoDF.loc[akeneoDF['OnGlobal'] == 'N','OnGlobal'] = False
            #akeneoDF.loc[akeneoDF['OnJet'] == 'N','OnJet'] = False
            akeneoDF.loc[akeneoDF['OnWalmart'] == 'N','OnWalmart'] = False
            akeneoDF.loc[akeneoDF['OnEbay'] == 'N','OnEbay'] = False

            #Akeneo needs a 'unit' companion 
            akeneoDF.loc[akeneoDF['ShippingWeight'].notnull(),'ShippingWeight-unit'] = 'POUND'

            #Not sure i need this....
            akeneoDF = akeneoDF.dropna(axis=0, how='all')

            #keeping track
            akeneoDF['From_Sage_Sync_Date'] = datetime.now().strftime('%Y-%m-%d')

            print('making json nests')

            #making JSON from df values...each of the different types of data need to formated slighly differently
            for cols in attributeCols:
                print(cols)
                akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = None, axis = 1)   
            print('attributeCols')

            for cols in currencyCols:
                print(cols)
                akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = 'USD', unit = None, axis = 1)     
            print('currencyCols')

            for cols in unitCols:
                print(cols)
                if 'weight' in cols or 'Weight' in cols:
                    akeneoUnit = 'POUND'
                else:
                    akeneoUnit = 'INCH'
                akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = akeneoUnit, axis = 1)
            print('unitCols')

            for cols in associationCols:
                print(cols)
                akeneoDF = akeneoDF.apply(make_json_association_data_nest, column_name = cols, axis = 1)  
            print('associationCols')
            
            valuesCols = attributeCols + currencyCols + unitCols
            
            #filtering out columns we aren't sending to akeneo (I think we did this a bunch already...not sure if needed) ... maybe an 'Akeneo Sync Date' consdieration
            akeneoDF = akeneoDF.loc[:, jsonCols]

            #More filtering.... this should take care of akeneo Sync Date or anything else I missed
            print(akeneoDF.shape[0])
            akeneoDF = akeneoDF.dropna(subset=akeneoDF.columns.difference(['identifier','enabled','From_Sage_Sync_Date']), how='all')
            print(akeneoDF.shape[0])
            if akeneoDF.shape[0] > 0:
            

                #This is the real JSON magic
                #We make a data frame essentially 3 columns, and an Index [ Identifier, enabled, and all the json values (as the Akenoe API needs it)]
                jsonDF = (akeneoDF.groupby(['identifier','enabled'], as_index=False)
                            .apply(lambda x: x[valuesCols].dropna(axis=1).to_dict('records'))
                            .reset_index()
                            .rename(columns={'':'values'}))
                #code above doesn't seem to work .rename(columns={'':'values'})).... this takes the last column and renames it to 'values'
                jsonDF.rename(columns={ jsonDF.columns[3]: "values" }, inplace = True)

                #Just as a backup in case something goes off the rails on it's way to akeneo
                jsonDF.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\AndrewsItemFiles\\jsonDF.csv', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL)

                #not sure we need this IF anymore  ... maybe an 'Akeneo Sync Date' consdieration
                if len(list(jsonDF)) != 0:

                    load_failure = False
                    #api_errors_file = open("Akeneo_Sync_Data_Errors.txt", "w") 
                    #KUACOUUA - Andrew
                    #KUAEL7RV - Anthony
                    #KUAAY4PZ - Kris
                    #KUAAZIJV - Travis    
                    print("made it to exit")
                    
                    #actually converting the dataframe to JSON
                    #Depending on which or both 'values' and 'enabled' have updated...we attempt to make the JSON
                    try:
                        try:
                            print('try enabled and values')
                            values_for_json = jsonDF.loc[:, ['identifier','enabled','values']].dropna(how='all',subset=['enabled','values']).to_dict(orient='records') 
                        except:
                            try:
                                print('...okay...try values')
                                values_for_json = jsonDF.loc[:, ['identifier','values']].dropna(how='all',subset=['values']).to_dict(orient='records')    
                            except:  
                                print('...okay...try enabled')           
                                values_for_json = jsonDF['identifier','enabled'].to_dict(orient='records') 
                        #we are sending the JSON now :D
                        data_results = akeneo.products.update_create_list(values_for_json)   
                        print("data results...")               
                        print(data_results)   
                    #if we failed to create JSON and/or send we log error and send it 
                    except requests.exceptions.RequestException as api_error:
                        print('crap...an error')
                        print(str(api_error))
                        load_failure = True
                        #api_errors_file.write(str(api_error))    

                    #If fail make wrike task                    
                    if load_failure == True:
                        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]' #Andrew
                        folderid = 'IEAAJKV3I4ZGKSRC' #Data Requests 
                        description = "Attached List of items failed while syncing to Akeneo due to Akeneo's Validation.\n\nThere should be a description of the error from the Akeneo API, along with the ItemCode.\n\n" + str(api_error)
                        response = makeWrikeTask(title = "Akeneo Sync Product Data Failure - " + date.today().strftime('%y/%m/%d'), description = description, assignees = assignees, folderid = folderid)
                        response_dict = json.loads(response.text)
                        taskid = response_dict['data'][0]['id']
                        #filetoattachpath = "Akeneo_Sync_Data_Errors.txt"
                        #print('Attaching file')
                        #attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
                        print('File attached!')
                    #If successfully created JSON and sent to akeneo make success task (we actually sent data to api)
                    else:
                        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]' #Andrew
                        folderid = 'IEAAJKV3I4ZGKSRC' #Data Requests 
                        description = "Success!" + "(" + str(jsonDF.shape[0]) + ")"
                        title = "Sage to Akeneo Product Sync Data Success! - " + date.today().strftime('%y/%m/%d')#
                        response = makeWrikeTask(title = title, description = description, assignees = assignees, folderid = folderid)  
                        response_dict = json.loads(response.text)    
                        taskid = response_dict['data'][0]['id']
                        markWrikeTaskComplete(taskid)      
                    
                    #Even if the JSON and data being sent are all formatted correctly... there could be addtional Akeneo validation that could reject specific items...this si how we handle this
                    #These IFs may make more sense as a CASE statement...leaving for now
                    print("reponses")
                    data_reponse_df = pd.DataFrame.from_dict(data_results)  
                    #Anything not in the 200 range is some sort of issue
                    errordf = data_reponse_df.loc[(data_reponse_df["status_code"] > 299) | (data_reponse_df["status_code"] < 200)]
                    print(data_reponse_df)

                    #If any valdiation errors...report those
                    if errordf.shape[0] > 0:
                        print('some items did not pass akeneo validation')
                        #shouldn't be a ton...so excel should be fine
                        errordf.to_excel('errordf.xlsx')
                        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'#,KUALCDZR,KUAEL7RV]' # Andrew, Anthony
                        folderid = 'IEAAJKV3I4JEW3BI' #Web Requests IEAAJKV3I4GOVKOA
                        wrikedescription = ""
                        wriketitle = "Sage to Akeneo Error - " + date.today().strftime('%y/%m/%d') + "(" + str(errordf.shape[0]) + ")"
                        response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
                        response_dict = json.loads(response.text)
                        taskid = response_dict['data'][0]['id']
                        filetoattachpath = 'errordf.xlsx'
                        print('Attaching file')
                        attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
                        print('File attached!')   
                    else:
                        print('no api data errors....Yay!')
                        todayakeneoDF.to_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\akeneolyzer\LastAkeneoLyzer')
                else:
                    print('no data need syncing to akeneo....Yay!')
                    todayakeneoDF.to_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\akeneolyzer\LastAkeneoLyzer')                
        else:
            print('nothing has changed ... :D')
            #todayakeneoDF.to_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\akeneolyzer\LastAkeneoLyzer')

#     # MarginChecking    on Wednesdays
    if date.today().weekday() == 2:

        print('# MarginChecking tag along for Wendnesdays')
        print(todayakeneoDF)
        todayakeneoDF['ShipWeight'] = todayakeneoDF['ShipWeight'].fillna(0.1)
        todayakeneoDF['UDF_ON_CLEARANCE'] = todayakeneoDF['UDF_ON_CLEARANCE'].fillna('N')

        todayakeneoDF['UDF_RETAIL_GM_CHK'] = (((todayakeneoDF['SuggestedRetailPrice']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['SuggestedRetailPrice'])*100).round(2)
        todayakeneoDF['UDF_STD_GM_CHK'] = (((todayakeneoDF['StandardUnitPrice']-todayakeneoDF['StandardUnitCost']+todayakeneoDF['ShippingEffect'])/todayakeneoDF['StandardUnitPrice'])*100).round(2)
        todayakeneoDF['UDF_GSA_GM_CHK'] = (((todayakeneoDF['UDF_GSA_PRICE']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_GSA_PRICE'])*100).round(2)
        todayakeneoDF['UDF_AMAZ002_MAX_PM_CHK'] = (((todayakeneoDF['UDF_AMAZ002_MAX']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_AMAZ002_MAX'])*100).round(2)
        todayakeneoDF['UDF_AMAZ002_MIN_PM_CHK'] = (((todayakeneoDF['UDF_AMAZ002_MIN']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_AMAZ002_MIN'])*100).round(2)
        todayakeneoDF['UDF_EBAY_PM_CHK'] = (((todayakeneoDF['UDF_EBAY_PRICE']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_EBAY_PRICE'])*100).round(2)
        todayakeneoDF['UDF_GLOBAL_PM_CHK'] = (((todayakeneoDF['UDF_GLOBAL_PRICE']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_GLOBAL_PRICE'])*100).round(2)
        todayakeneoDF['UDF_SEARS_PM_CHK'] = (((todayakeneoDF['UDF_SEARS_PRICE']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_SEARS_PRICE'])*100).round(2)
        todayakeneoDF['UDF_WALMART_PM_CHK'] = (((todayakeneoDF['UDF_WALMART_PRICE']-todayakeneoDF['StandardUnitCost'])/todayakeneoDF['UDF_WALMART_PRICE'])*100).round(2)  

        todayakeneoDF['ProfitAfterShip'] = (todayakeneoDF['StandardUnitPrice']-todayakeneoDF['StandardUnitCost']+todayakeneoDF['ShippingEffect'])

        todayakeneoDF.replace([np.inf, -np.inf], np.nan, inplace=True)
        todayakeneoDF = todayakeneoDF.fillna(0)

        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['SuggestedRetailPrice'] == 0)), 'UDF_RETAIL_GM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['StandardUnitPrice'] == 0)), 'UDF_STD_GM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | ((todayakeneoDF['UDF_GSA_PRICE'] == 0) & (todayakeneoDF['UDF_GSA_TEMP_PRICE'] == 0))), 'UDF_GSA_GM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_AMAZ002_MAX_PM'] == 0)), 'UDF_AMAZ002_MAX_PM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_AMAZ002_MIN_PM'] == 0)), 'UDF_AMAZ002_MIN_PM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_EBAY_PRICE'] == 0)), 'UDF_EBAY_PM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_GLOBAL_PRICE'] == 0)), 'UDF_GLOBAL_PM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_SEARS_PRICE'] == 0)), 'UDF_SEARS_PM_CHK'] = 0
        todayakeneoDF.loc[((todayakeneoDF['StandardUnitCost'] == 0) | (todayakeneoDF['UDF_WALMART_PRICE'] == 0)), 'UDF_WALMART_PM_CHK'] = 0

        todayakeneoDF.loc[(((todayakeneoDF['UDF_GSA_TEMP_PRICE']) != 0) & ((todayakeneoDF['UDF_GSA_PRICE']) > (todayakeneoDF['UDF_GSA_TEMP_PRICE']))), 'UDF_GSA_PRICE'] = todayakeneoDF['UDF_GSA_TEMP_PRICE']

        todayakeneoDF['Margin ALERT'] = ''
        todayakeneoDF.loc[((todayakeneoDF['UDF_STD_GM_CHK'] < 14) & (todayakeneoDF['ProfitAfterShip'] < 6) & (todayakeneoDF['UDF_ON_CLEARANCE'] == 'N') & (todayakeneoDF['StandardUnitPrice'] != 0) & (todayakeneoDF['StandardUnitPrice'] != todayakeneoDF['SuggestedRetailPrice'])), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Sale'"
        todayakeneoDF.loc[((todayakeneoDF['UDF_STD_GM_CHK'] < 11) & (todayakeneoDF['ProfitAfterShip'] < 12) & (todayakeneoDF['UDF_ON_CLEARANCE'] == 'N') & (todayakeneoDF['StandardUnitPrice'] != 0) & (todayakeneoDF['StandardUnitPrice'] != todayakeneoDF['SuggestedRetailPrice'])), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Sale'"
        todayakeneoDF.loc[((todayakeneoDF['UDF_STD_GM_CHK'] < 8) & (todayakeneoDF['ProfitAfterShip'] < 18) & (todayakeneoDF['UDF_ON_CLEARANCE'] == 'N') & (todayakeneoDF['StandardUnitPrice'] != 0) & (todayakeneoDF['StandardUnitPrice'] != todayakeneoDF['SuggestedRetailPrice'])), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Sale'"
        todayakeneoDF.loc[((todayakeneoDF['UDF_STD_GM_CHK'] < 5) & (todayakeneoDF['ProfitAfterShip'] < 25) & (todayakeneoDF['UDF_ON_CLEARANCE'] == 'N') & (todayakeneoDF['StandardUnitPrice'] != 0) & (todayakeneoDF['StandardUnitPrice'] != todayakeneoDF['SuggestedRetailPrice'])), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Sale'"        
        todayakeneoDF.loc[(((todayakeneoDF['UDF_AMAZ002_MAX_PM_CHK']) < (todayakeneoDF['UDF_AMAZ002_MIN_PM_CHK'])) & (todayakeneoDF['UDF_AMAZ002_MIN'] != 0)& (todayakeneoDF['UDF_ISAMAZ002'] == 'Y')), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'AmazonMax'"
        todayakeneoDF.loc[(((todayakeneoDF['UDF_AMAZ002_MIN_PM_CHK']) < 16) & (todayakeneoDF['UDF_AMAZ002_MIN_PM'] != 0) & (todayakeneoDF['UDF_AMAZ002_MIN'] != 0) & (todayakeneoDF['UDF_ISAMAZ002'] == 'Y')), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'AmazonMin'"
        todayakeneoDF.loc[(((todayakeneoDF['UDF_EBAY_PM_CHK']) < 16) & (todayakeneoDF['UDF_EBAY_PRICE'] != 0) & (todayakeneoDF['UDF_ISEBAY'] == 'Y')), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Ebay'"
        todayakeneoDF.loc[((((todayakeneoDF['UDF_GLOBAL_PM_CHK']) < 7 ) | ((todayakeneoDF['UDF_GLOBAL_PM_CHK']) > 30)) & (todayakeneoDF['UDF_GLOBAL_PRICE'] != 0) & (todayakeneoDF['UDF_ISGLOBAL'] == 'Y')), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Global'"
        todayakeneoDF.loc[(((todayakeneoDF['UDF_SEARS_PM_CHK']) < 20) & (todayakeneoDF['UDF_SEARS_PRICE'] != 0)), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Sears'"
        todayakeneoDF.loc[(((todayakeneoDF['UDF_WALMART_PM_CHK']) < 20) & (todayakeneoDF['UDF_WALMART_PRICE'] != 0)) & (todayakeneoDF['UDF_ISWALMART'] == 'Y'), 'Margin ALERT'] = todayakeneoDF['Margin ALERT'] + "'Walmart'"
        todayakeneoDF.replace("", np.nan, inplace=True)
        MarginAlertDF = todayakeneoDF[((todayakeneoDF['Margin ALERT'].notna()) & (todayakeneoDF['InactiveItem'] != 'Y'))]
        MarginAlertDF = MarginAlertDF.query("StandardUnitCost > 0")
        MarginAlertDF = MarginAlertDF.query("`QuantityOnHand-BVA` == 0")
        MarginAlertDF = MarginAlertDF.query("`QuantityOnHand-BVU` == 0")
        MarginAlertDF = MarginAlertDF.query("`UDF_PRICE_STATUS_CODE` != 'LIQUIDATE'")
        MarginAlertDF = MarginAlertDF.query("`UDF_PRICE_STATUS_CODE` != 'REBALANCE'")

        if MarginAlertDF.shape[0] > 0:
            print('there are things to checking margins for...')
            MarginAlertDF.to_excel('\\\\FOT00WEB\\Alt Team\\Kris\\GitHubRepos\\akeneolyzer\\MarginAlertDF.xlsx')
            print('some items have margin alerts')
            assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]' # Andrew, Anthony
            folderid = 'IEAAJKV3I4JEW3BI' #Web Requests IEAAJKV3I4GOVKOA
            wrikedescription = "Attached List of items that Anthony wants margin Alerts on. The last column has the alerts!"
            wriketitle = "Akeneo SAD Margin Check - " + date.today().strftime('%y/%m/%d') + "(" + str(MarginAlertDF.shape[0]) + ")"
            response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
            response_dict = json.loads(response.text)
            taskid = response_dict['data'][0]['id']
            filetoattachpath = '\\\\FOT00WEB\\Alt Team\\Kris\\GitHubRepos\\akeneolyzer\\MarginAlertDF.xlsx'
            print('Attaching file')
            attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
            print('File attached!')           