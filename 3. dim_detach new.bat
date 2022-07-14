# %%
import numpy as np
import pandas as pd
import random
import time
import string

import datetime
 
from datetime import timedelta
import hashlib

# %%
f_order = pd.read_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_movement = pd.read_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_purchase = pd.read_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_sales = pd.read_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_stockholding = pd.read_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_transaction = pd.read_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')

# %%
def create_hash(df):
    df["hash"] = ""
    for col in df.columns:
        df["hash"] = df["hash"] + df[col].astype("str")
    df["hash"] = df["hash"].apply(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())

# %%
f_order = f_order.iloc[:,2:]
f_movement = f_movement.iloc[:,2:]
f_purchase = f_purchase.iloc[:,2:]
f_sales = f_sales.iloc[:,2:]
f_stockholding = f_stockholding.iloc[:,2:]
f_transaction = f_transaction.iloc[:,2:]

# %% [markdown]
# #### Stock Item

# %%
lst = []
for i in range(len(f_movement)):
    lst.append(f_movement.loc[i,['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]])
for i in range(len(f_order)):
    lst.append(f_order.loc[i,['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]])
for i in range(len(f_sales)):
    lst.append(f_sales.loc[i,['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]])
for i in range(len(f_stockholding)):
    lst.append(f_stockholding.loc[i,['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]])
for i in range(len(f_purchase)):
    lst.append(f_purchase.loc[i,['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]])
    
dim_stock_item = pd.DataFrame(lst,columns=['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]).reset_index(drop=True).drop_duplicates()

lst = []
for key in dim_stock_item["StockItemKey"].unique():
    lst.append(dim_stock_item.loc[dim_stock_item["StockItemKey"] == key].head(1).reset_index(drop = True).T.squeeze().values)
    
dim_stock_item = pd.DataFrame(lst,columns=['StockItemKey','WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice','TypicalWeightPerUnit',]).sort_values("StockItemKey").reset_index(drop=True)
create_hash(dim_stock_item)

# %% [markdown]
# #### Dim.Supplier

# %%
lst = []
for i in range(len(f_movement)):
    lst.append(f_movement.loc[i,['SupplierKey','WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode']])
for i in range(len(f_transaction)):
    lst.append(f_transaction.loc[i,['SupplierKey','WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode']])
for i in range(len(f_purchase)):
    lst.append(f_purchase.loc[i,['SupplierKey','WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode']])
    
dim_supplier = pd.DataFrame(lst,columns=['SupplierKey','WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode']).reset_index(drop=True).drop_duplicates()

lst = []
for key in dim_supplier["SupplierKey"].unique():
    lst.append(dim_supplier.loc[dim_supplier["SupplierKey"] == key].head(1).reset_index(drop = True).T.squeeze().values)
    
dim_supplier = pd.DataFrame(lst,columns=['SupplierKey','WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode']).sort_values("SupplierKey").reset_index(drop=True)
create_hash(dim_supplier)

# %% [markdown]
# #### Dim.Customer

# %%
lst = []
for i in range(len(f_transaction)):
    lst.append(f_transaction.loc[i,['CustomerKey','WWICustomerID'
,'Customer','BillToCustomer','Category','BuyingGroup','PrimaryContact','PostalCode']])
    
for i in range(len(f_sales)):
    lst.append(f_sales.loc[i,['CustomerKey','WWICustomerID'
,'Customer','BillToCustomer','Category','BuyingGroup','PrimaryContact','PostalCode']])

# %%
dim_customer = pd.DataFrame(lst,columns=['CustomerKey','WWICustomerID'
,'Customer','BillToCustomer','Category','BuyingGroup','PrimaryContact','PostalCode','ValidFrom','ValidTo']).reset_index(drop=True).drop_duplicates()


# %%
lst = []
for key in dim_customer["CustomerKey"].unique():
    lst.append(dim_customer.loc[dim_customer["CustomerKey"] == key].head(1).reset_index(drop = True).T.squeeze().values)

# %%
dim_customer = pd.DataFrame(lst,columns=['CustomerKey','WWICustomerID'
,'Customer','BillToCustomer','Category','BuyingGroup','PrimaryContact','PostalCode','ValidFrom','ValidTo']).sort_values("CustomerKey").reset_index(drop=True)
create_hash(dim_customer)

# %%
#generate a random date between two other dates
 
def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format,time.localtime(ptime))
 
def random_date(start, end, prop):    
    return str_time_prop(start, end, '%Y-%m-%d', prop)

# %%
def generate_data(df,field,min_value,max_value,line_number,option = None):
    if option == 'float':
        df[field] = pd.Series([round(random.uniform(min_value,max_value),2) for _ in range(line_number)])
    else:
        if(option == 'date'):
            df[field] = pd.Series([random_date(min_value, max_value, random.random()) for _ in range(line_number)])
        else:
            df[field] = pd.Series([random.randint(min_value,max_value) for _ in range(line_number)])
    return df[field]

# %%
dim_customer['ValidFrom'] = datetime.date.today()
dim_customer['ValidTo'] = datetime.date(2030,12,31)
dim_customer['LineageKey'] = 2

dim_stock_item['ValidFrom'] = datetime.date.today()
dim_stock_item['ValidTo'] = datetime.date(2030,12,31)
dim_stock_item['LineageKey'] = 5

dim_supplier['ValidFrom'] = datetime.date.today()
dim_supplier['ValidTo'] = datetime.date(2030,12,31)
dim_supplier['LineageKey'] = 6

# %%
dim_customer['Current'] = 1
dim_stock_item['Current'] = 1
dim_supplier['Current'] = 1

# %%
dim_customer.to_csv(f'./bang_moi/Dim_Customer/Dim_Customer ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv' ,index=False)
dim_stock_item.to_csv(f'./bang_moi/Dim_Stock_item/Dim_Stock_item ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
dim_supplier.to_csv(f'./bang_moi/Dim_Supplier/Dim_Supplier ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)


