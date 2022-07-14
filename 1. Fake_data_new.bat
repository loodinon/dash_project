# %%
import numpy as np
import pandas as pd
import random
import time
import string
import csv
import datetime
 
from datetime import timedelta

# %%
d_city = pd.read_csv(f'./bang_goc/Dim_City.csv')
d_employee = pd.read_csv(f'./bang_goc/Dim_Employee.csv')
d_paymentmethod = pd.read_csv(f'./bang_goc/Dim_Payment_method.csv')
d_transactiontype = pd.read_csv(f'./bang_goc/Dim_Transaction_type.csv')

# %%
BillToCustomer_list = pd.read_csv(f'./list/BillToCustomer_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Brand_list = pd.read_csv(f'./list/Brand_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Buying_package_list= pd.read_csv(f'./list/Buying_package_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
BuyingGroup_list= pd.read_csv(f'./list/BuyingGroup_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Category_list_Customer = pd.read_csv(f'./list/Category_list (Customer).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Category_list_Supplier = pd.read_csv(f'./list/Category_list (Supplier).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Customer_list= pd.read_csv(f'./list/Customer_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Description_list_Order= pd.read_csv(f'./list/Description_list (Order).csv',encoding = 'ISO-8859-1',header=None).iloc[:,0].to_list()
Description_list_Sale= pd.read_csv(f'./list/Description_list (Sale).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Package_list_Order= pd.read_csv(f'./list/Package_list (Order).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Package_list_Purchase= pd.read_csv(f'./list/Package_list (Purchase).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Package_list_Sale= pd.read_csv(f'./list/Package_list (Sale).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
PrimaryContact_list_Customer= pd.read_csv(f'./list/PrimaryContact_list (Customer).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
PrimaryContact_list_Supplier= pd.read_csv(f'./list/PrimaryContact_list (Supplier).csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Selling_package_list= pd.read_csv(f'./list/Selling_package_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Color_list= pd.read_csv(f'./list/Color_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Size_list= pd.read_csv(f'./list/Size_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Stock_item_list= pd.read_csv(f'./list/Stock_item_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
Supplier_list= pd.read_csv(f'./list/Supplier_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()
SupplierReference_list= pd.read_csv(f'./list/SupplierReference_list.csv',encoding = 'utf-8',header=None).iloc[:,0].to_list()

# %%
df_Movement=pd.DataFrame(columns=[['MovementKey','DateKey','StockItemKey','CustomerKey',
'SupplierKey','TransactionTypeKey','WWIStockItemTransactionID',
'WWIInvoiceID','WWIPurchaseOrderID','Quantity','LineageKey',
'WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode',
'WWICustomerID','Customer','BillToCustomer','Category','BuyingGroup',
'PrimaryContact','PostalCode',
'WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice',
'TypicalWeightPerUnit','ValidFrom','ValidTo']])

# %%
df_Order=pd.DataFrame(columns=[['OrderKey','CityKey','CustomerKey','StockItemKey','OrderDateKey',
'PickedDateKey','SalespersonKey','PickerKey','WWIOrderID','WWIBackorderID',
'Description','Package','UnitPrice','TaxRate','TotalExcludingTax','TaxAmount',
'TotalIncludingTax','LineageKey',
                                
'WWIEmployeeID(Salesperson)','Employee(Salesperson)','PreferredName(Salesperson)',
'WWIEmployeeID(Picker)','Employee(Picker)','PreferredName(Picker)',
'WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice',
'TypicalWeightPerUnit',
'WWICustomerID','Customer','BillToCustomer','Category','BuyingGroup',
'PrimaryContact','PostalCode','ValidFrom','ValidTo']])

# %%
df_Purchase=pd.DataFrame(columns=[['PurchaseKey','DateKey','SupplierKey','StockItemKey',
'TransactionTypeKey','WWIPurchaseOrderID','OrderedOuters','OrderedQuantity','ReceivedOuters','Package',
'IsOrderFinalized','LineageKey',
'WWISupplierID','Supplier','Category','PrimaryContact','SupplierReference',
'PaymentDays','PostalCode',
'WWIStockItemID','StockItem','Color','SellingPackage','BuyingPackage','Brand','Size','LeadTimeDays',
'QuantityPerOuter','IsChillerStock','Barcode','TaxRate','UnitPrice','RecommendedRetailPrice',
'TypicalWeightPerUnit','ValidFrom','ValidTo']])

# %%
fac_sales=pd.DataFrame(columns=[['SaleKey',
'CityKey',
'CustomerKey',
'BillToCustomerKey',
'StockItemKey',
'InvoiceDateKey',
'DeliveryDateKey',
'SalespersonKey',
'WWIInvoiceID',
'Package',
'UnitPrice',
'TaxRate',
'TotalExcludingTax',
'TaxAmount',
'Profit',
'TotalIncludingTax',
'TotalDryItems',
'TotalChillerItems',
'Description',
'WWIStockItemID',
'Color',
'SellingPackage',
'BuyingPackage',
'Brand',
'Size',
'LeadTimeDays',
'QuantityPerOuter',
'IsChillerStock',
'Barcode',
'TaxRate',
'UnitPrice',
'RecommendedRetailPrice',
'TypicalWeightPerUnit',
'StockItem',
'CustomerKey',
'WWICustomerID',
'Customer',
'BillToCustomer',
'Category',
'BuyingGroup',
'PrimaryContact',
'PostalCode',
'ValidFrom',
'ValidTo',
'LineageKey']])

# %%
fac_stockholding=pd.DataFrame(columns=[['StockHoldingKey',
'StockItemKey',
'QuantityOnHand',
'BinLocation',
'LastStocktakeQuantity',
'LastCostPrice',
'ReorderLevel',
'TargetStockLevel',
'LineageKey',
#Stockitemkey
'WWIStockItemID',
"StockItem",
'Color',
'SellingPackage',
'BuyingPackage',
'Brand',
'Size',
'LeadTimeDays',
'QuantityPerOuter',
'IsChillerStock',
'Barcode',
'TaxRate',
'UnitPrice',
'RecommendedRetailPrice',
'TypicalWeightPerUnit',
'ValidFrom',
'ValidTo']])

# %%
Fac_transaction=pd.DataFrame(columns=[['TransactionKey',
'DateKey',
'CustomerKey',
'BillToCustomerKey',
'SupplierKey',
'TransactionTypeKey',
'PaymentMethodKey',
'WWICustomerTransactionID',
'WWISupplierTransactionID',
'WWIInvoiceID',
'WWIPurchaseOrderID',
'SupplierInvoiceNumber',
'TotalExcludingTax',
'TaxAmount',
'TotalIncludingTax',
'OutstandingBalance',
'IsFinalized',
'WWICustomerID',
'Customer',
'BillToCustomer',
'Category',
'BuyingGroup',
'PrimaryContact',
'PostalCode',
#'SupplierKey',
'WWISupplierID',
'Supplier',
'Category',
'PrimaryContact',
'SupplierReference',
'PaymentDays',
'PostalCode',

'ValidFrom',
'ValidTo',
'LineageKey']])

# %%
N = 1000

# %%
#generate a random date between two other dates
 
def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    time_format = random.choice(['%Y-%m-%d','%Y/%m/%d','%m-%d-%Y','%d %b %Y'])
    return time.strftime(time_format, time.localtime(ptime))
 
def random_date(start, end, prop):    
    return str_time_prop(start, end, '%Y-%m-%d', prop)

# %%
def generate_data(df,field,min_value,max_value,option = None,line_number = N):
    if option == 'float':
        df[field] = pd.Series([round(random.uniform(min_value,max_value),2) for _ in range(line_number)])
    else:
        if(option == 'date'):
            df[field] = pd.Series([random_date(min_value, max_value, random.random()) for _ in range(line_number)])
        else:
            df[field] = pd.Series([random.randint(min_value,max_value) for _ in range(line_number)])
    return df[field]

# %%
def generate_unique_data(df,field,min_value,max_value,option = None,line_number = N):
    while len(
        generate_data(df,field,min_value,max_value,option = None,line_number = N).squeeze().unique()
    ) < line_number:
        return generate_unique_data(df,field,min_value,max_value,option = None,line_number = N)

# %%
def df2_random_choice_from_df1(df1,field1,df2,field2,line_number = N):
    temp = list(set(df1[field1].values.ravel()))
    df2[field2] = pd.Series([random.choice(temp) for _ in range(line_number)])
    return df2[field2]

# %%
def random_choice_from_list(df,field,l,line_number = N,option = None):
    if option == 'add number':
        df[field] = pd.Series([str(random.randint(0,400)) + " " +random.choice(l) for _ in range(line_number)])
    else:
        df[field] = pd.Series([random.choice(l) for _ in range(line_number)])
    return df[field]

# %%
def fake_string(head,length,option,line_number = N):
    if option == 'letter + digit':
        return pd.Series([head + ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k = length)) for _ in range(line_number)])
    if option == 'digit':
        return pd.Series([head + ''.join(random.choices(string.digits, k = length)) for _ in range(line_number)])

# %% [markdown]
# ### Fact.Movement
# 

# %%
#df_Movement

generate_unique_data(df_Movement,'MovementKey',0,100000)
generate_data(df_Movement,'DateKey',"2016-06-01",str(datetime.date.today()),option='date')
generate_data(df_Movement,'StockItemKey',1,671)
generate_data(df_Movement,'CustomerKey',1,40)
generate_data(df_Movement,'SupplierKey',1,50)
generate_data(df_Movement,'TransactionTypeKey',1,14)
generate_data(df_Movement,'WWIStockItemTransactionID',0,10000)
generate_data(df_Movement,'WWIInvoiceID',0,10000)
generate_data(df_Movement,'WWIPurchaseOrderID',0,1000)
generate_data(df_Movement,'Quantity',-360,70000)
generate_data(df_Movement,'ValidFrom',"2016-06-01","2019-06-01",option='date')
generate_data(df_Movement,'ValidTo',"2019-06-01","2300-12-31",option='date');
df_Movement['LineageKey']=8

# %% [markdown]
# Fact.Order

# %%
#df_Order
#Fact.Order
generate_data(df_Order,'OrderKey',0,100000)
generate_data(df_Order,'CityKey',1,116294)
#df2_random_choice_from_df1(d_city,'CityKey',df_Order,'CityKey')
generate_data(df_Order,'StockItemKey',1,671)
df2_random_choice_from_df1(df_Movement,'CustomerKey',df_Order,'CustomerKey')
df2_random_choice_from_df1(df_Movement,'DateKey',df_Order,'OrderDateKey')
df_Order['PickedDateKey']=df_Order['OrderDateKey'].values


random_choice_from_list(df_Order,'SalespersonKey', d_employee[d_employee["IsSalesperson"] == True]["EmployeeKey"].to_list())
random_choice_from_list(df_Order,'PickerKey', d_employee[d_employee["IsSalesperson"] == False]["EmployeeKey"].to_list())
generate_data(df_Order,'WWIOrderID',0,75000)
generate_data(df_Order,'WWIBackorderID',0,75000)

random_choice_from_list(df_Order,'Description',Description_list_Order)

random_choice_from_list(df_Order,'Package',Package_list_Order,option='add number')

generate_data(df_Order,'UnitPrice',0,1900.00,option='float')
generate_data(df_Order,'TaxRate',10.000,15.000,option='float')
generate_data(df_Order,'TotalExcludingTax',2.50,1900.00,option='float')
generate_data(df_Order,'TaxAmount',0.00,3000.00,option='float')
generate_data(df_Order,'TotalIncludingTax',2.00,22000.00,option='float')
df_Order['LineageKey']=9

#Dim.Stock Item
generate_data(df_Order,'WWIStockItemID',0,300)


random_choice_from_list(df_Order,'StockItem',Stock_item_list)

random_choice_from_list(df_Order,'Color',Color_list)

random_choice_from_list(df_Order,'SellingPackage',Selling_package_list)

random_choice_from_list(df_Order,'BuyingPackage',Buying_package_list)

random_choice_from_list(df_Order,'Brand',Brand_list)

random_choice_from_list(df_Order,'Size',Size_list)

generate_data(df_Order,'LeadTimeDays',0,25)
generate_data(df_Order,'QuantityPerOuter',0,40)
generate_data(df_Order,'IsChillerStock',0,1)

df_Order['Barcode']=fake_string('8792838293',3,option='digit')

generate_data(df_Order,'TaxRate',0.000,20.000,option='float')
generate_data(df_Order,'UnitPrice',0.00,2000.00,option='float')
generate_data(df_Order,'RecommendedRetailPrice',0.00,3000.00,option='float')
generate_data(df_Order,'TypicalWeightPerUnit',0.000,21.000,option='float')

generate_data(df_Order,'ValidFrom',"2016-06-01","2019-06-01",option='date')
generate_data(df_Order,'ValidTo',"2019-06-01","2300-12-31",option='date');

# %% [markdown]
# ### Fact.Purchase

# %%
#df_Purchase

generate_data(df_Purchase,'PurchaseKey',0,100000)

df2_random_choice_from_df1(df_Movement,'DateKey',df_Purchase,'DateKey')
df2_random_choice_from_df1(df_Movement,'SupplierKey',df_Purchase,'SupplierKey')
df2_random_choice_from_df1(df_Movement,'StockItemKey',df_Purchase,'StockItemKey')

generate_data(df_Purchase,'WWIPurchaseOrderID',1,3000)
generate_data(df_Purchase,'OrderedOuters',1,4000)
generate_data(df_Purchase,'OrderedQuantity',1,70000)
generate_data(df_Purchase,'ReceivedOuters',1,4000)

random_choice_from_list(df_Purchase,'Package',Package_list_Purchase)
#generate_data(df_Purchase,'Package',0,10000)

generate_data(df_Purchase,'IsOrderFinalized',0,1)

df_Purchase['LineageKey']=10

generate_data(df_Purchase,'ValidFrom',"2016-06-01","2019-06-01",option='date')
generate_data(df_Purchase,'ValidTo',"2019-06-01","2300-12-31",option='date');

# %% [markdown]
# ### Fact.Sale

# %%
generate_data(fac_sales,'SaleKey',2000,4000)
generate_data(fac_sales,'CityKey',1,116294)
#df2_random_choice_from_df1(d_city,'CityKey',fac_sales,'CityKey')
df2_random_choice_from_df1(df_Movement,'CustomerKey',fac_sales,'CustomerKey')
generate_data(fac_sales,'BillToCustomerKey',0,250)
df2_random_choice_from_df1(df_Order,'StockItemKey',fac_sales,'StockItemKey')
generate_data(fac_sales,'InvoiceDateKey',"2016-06-01","2022-06-01",option='date')
fac_sales['DeliveryDateKey']=(fac_sales['InvoiceDateKey'].astype('datetime64') + timedelta(days = 1)).squeeze()
df2_random_choice_from_df1(df_Order,'SalespersonKey',fac_sales,'SalespersonKey')
generate_data(fac_sales,'WWIInvoiceID',0,80000)
Description_list_Sale = ['USB food flash drive - hamburger',
                    '"The Gu" red shirt XML tag t-shirt (Black) 7XL',
                    'Halloween zombie mask (Light Brown) L',
                    '"The Gu" red shirt XML tag t-shirt (White) M',
                    'USB rocket launcher (Gray)']
random_choice_from_list(fac_sales,'Description',Description_list_Sale)

random_choice_from_list(fac_sales,'Package',Package_list_Sale,option='add number')

generate_data(fac_sales,'UnitPrice',0.00,1900.00,'float')
generate_data(fac_sales,'TaxRate',15,10.000,'float')
generate_data(fac_sales,'TotalExcludingTax',2.50,19000.00,'float')
generate_data(fac_sales,'TaxAmount',0.00,2900.00,'float')
generate_data(fac_sales,'Profit',-700,9200.00,'float')
fac_sales['TotalIncludingTax']=fac_sales['TotalExcludingTax'].values+fac_sales['TaxAmount'].values
generate_data(fac_sales,'TotalDryItems',0,400)
generate_data(fac_sales,'TotalChillerItems',0,300)
fac_sales['LineageKey']=11

#Dim.Customer
fac_sales['WWICustomerID']=fac_sales['CustomerKey'].values

random_choice_from_list(fac_sales,'Customer',Customer_list)

random_choice_from_list(fac_sales,'BillToCustomer',BillToCustomer_list)
              

random_choice_from_list(fac_sales,'Category',Category_list_Customer)
            
random_choice_from_list(fac_sales,'BuyingGroup',BuyingGroup_list)
              
random_choice_from_list(fac_sales,'PrimaryContact',PrimaryContact_list_Customer)

fac_sales['PostalCode'] = fake_string('9',4,'digit')

generate_data(fac_sales,'ValidFrom',"2016-06-01", "2022-06-01",option = 'date')
generate_data(fac_sales,'ValidTo',"2016-06-01", "2022-06-01",option = 'date');

# %% [markdown]
# ### Fact.StockHolding

# %%
generate_data(fac_stockholding,'StockHoldingKey',1,400000)
generate_data(fac_stockholding,'StockItemKey',1,400000)
generate_data(fac_stockholding,'QuantityOnHand',10000,50000)
generate_data(fac_stockholding,'BinLocation',1,10)
generate_data(fac_stockholding,'LastStocktakeQuantity',5000,30000)
generate_data(fac_stockholding,'LastCostPrice',4,200)
generate_data(fac_stockholding,'ReorderLevel',10,100)
generate_data(fac_stockholding,'TargetStockLevel',10,500)
generate_data(fac_stockholding,'ValidFrom',"2016-06-01","2019-06-01",option='date')
generate_data(fac_stockholding,'ValidTo',"2019-06-01","2300-12-31",option='date');
generate_data(fac_stockholding,'LineageKey',12,12)
#StockItem
df2_random_choice_from_df1(df_Order,'WWIStockItemID',fac_stockholding,'WWIStockItemID')
df2_random_choice_from_df1(df_Order,'StockItem',fac_stockholding,'StockItem')
df2_random_choice_from_df1(df_Order,'Color',fac_stockholding,'Color')
df2_random_choice_from_df1(df_Order,'SellingPackage',fac_stockholding,'SellingPackage')
random_choice_from_list(fac_stockholding,'BuyingPackage',Buying_package_list)
#df2_random_choice_from_df1(df_Order,'Brand',fac_stockholding,'Brand')
random_choice_from_list(fac_stockholding,'Brand',Brand_list)
df2_random_choice_from_df1(df_Order,'Size',fac_stockholding,'Size')
df2_random_choice_from_df1(df_Order,'LeadTimeDays',fac_stockholding,'LeadTimeDays')
df2_random_choice_from_df1(df_Order,'QuantityPerOuter',fac_stockholding,'QuantityPerOuter')
df2_random_choice_from_df1(df_Order,'IsChillerStock',fac_stockholding,'IsChillerStock')
df2_random_choice_from_df1(df_Order,'Barcode',fac_stockholding,'Barcode')
df2_random_choice_from_df1(df_Order,'TaxRate',fac_stockholding,'TaxRate')
df2_random_choice_from_df1(df_Order,'UnitPrice',fac_stockholding,'UnitPrice')
df2_random_choice_from_df1(df_Order,'RecommendedRetailPrice',fac_stockholding,'RecommendedRetailPrice')
df2_random_choice_from_df1(df_Order,'TypicalWeightPerUnit',fac_stockholding,'TypicalWeightPerUnit');

# %%
#randomFact_transaction

generate_data(Fac_transaction,'TransactionKey',0,300000)
generate_data(Fac_transaction,'DateKey',"2016-06-01","2022-06-01",option='date')
generate_data(Fac_transaction,'CustomerKey',500,1000)
generate_data(Fac_transaction,'BillToCustomerKey',10,20)
generate_data(Fac_transaction,'SupplierKey',1,50)
generate_data(Fac_transaction,'TransactionTypeKey',1,14)
generate_data(Fac_transaction,'PaymentMethodKey',1,5)
generate_data(Fac_transaction,'WWICustomerTransactionID',4000,5000)
generate_data(Fac_transaction,'WWISupplierTransactionID',1500,2000)
generate_data(Fac_transaction,'WWIInvoiceID',1000,2000)
generate_data(Fac_transaction,'WWIPurchaseOrderID',1000,2000)
generate_data(Fac_transaction,'SupplierInvoiceNumber',6000,7000)
generate_data(Fac_transaction,'TotalExcludingTax',0,100)
generate_data(Fac_transaction,'TaxAmount',0,100)
Fac_transaction['TotalIncludingTax']=Fac_transaction['TaxAmount'].values+Fac_transaction['TotalExcludingTax'].values
generate_data(Fac_transaction,'OutstandingBalance',1,1)
generate_data(Fac_transaction,'IsFinalized',1,1)
generate_data(Fac_transaction,'WWICustomerID',700,1000)
random_choice_from_list(Fac_transaction,'Customer',Customer_list)
#Fac_transaction['BillToCustomer']='TailspinToys(HeadOffice)'
random_choice_from_list(Fac_transaction,'Category',Category_list_Supplier)
random_choice_from_list(Fac_transaction,'BillToCustomer',BillToCustomer_list)
random_choice_from_list(Fac_transaction,'BuyingGroup',BuyingGroup_list)
random_choice_from_list(Fac_transaction,'PrimaryContact',PrimaryContact_list_Customer)
generate_data(Fac_transaction,'PostalCode',90100,90900)
generate_data(Fac_transaction,'ValidFrom',"2016-06-01","2022-06-01",option='date')
generate_data(Fac_transaction,'ValidTo',"2016-06-01","2022-06-01",option='date')
generate_data(Fac_transaction,'WWISupplierID',14,25)
random_choice_from_list(Fac_transaction,'Supplier',Supplier_list)
random_choice_from_list(Fac_transaction,'Category',Category_list_Customer)
random_choice_from_list(Fac_transaction,'PrimaryContact',PrimaryContact_list_Supplier)
random_choice_from_list(Fac_transaction,'SupplierReference',SupplierReference_list)
generate_data(Fac_transaction,'PaymentDays',0,30)
generate_data(Fac_transaction,'PostalCode',90100,90800)
Fac_transaction['LineageKey'] = 13

# %% [markdown]
# UpdateFact.Movement

# %%
#Stock_Item
df2_random_choice_from_df1(df_Order,'WWIStockItemID',df_Movement,'WWIStockItemID')
df2_random_choice_from_df1(df_Order,'StockItem',df_Movement,'StockItem')
df2_random_choice_from_df1(df_Order,'Color',df_Movement,'Color')
df2_random_choice_from_df1(df_Order,'SellingPackage',df_Movement,'SellingPackage')
random_choice_from_list(df_Movement,'BuyingPackage',Buying_package_list)
random_choice_from_list(df_Movement,'Brand',Brand_list)

df2_random_choice_from_df1(df_Order,'Size',df_Movement,'Size')
df2_random_choice_from_df1(df_Order,'LeadTimeDays',df_Movement,'LeadTimeDays')
df2_random_choice_from_df1(df_Order,'QuantityPerOuter',df_Movement,'QuantityPerOuter')
df2_random_choice_from_df1(df_Order,'IsChillerStock',df_Movement,'IsChillerStock')
df2_random_choice_from_df1(df_Order,'Barcode',df_Movement,'Barcode')
df2_random_choice_from_df1(df_Order,'TaxRate',df_Movement,'TaxRate')
df2_random_choice_from_df1(df_Order,'UnitPrice',df_Movement,'UnitPrice')
df2_random_choice_from_df1(df_Order,'RecommendedRetailPrice',df_Movement,'RecommendedRetailPrice')
df2_random_choice_from_df1(df_Order,'TypicalWeightPerUnit',df_Movement,'TypicalWeightPerUnit')
# df2_random_choice_from_df1(df_Order,'ValidFrom(StockItem)',df_Movement,'ValidFrom(StockItem)')
# df2_random_choice_from_df1(df_Order,'ValidTo(StockItem)',df_Movement,'ValidTo(StockItem)')
df2_random_choice_from_df1(Fac_transaction,'WWISupplierID',df_Movement,'WWISupplierID')
df2_random_choice_from_df1(Fac_transaction,'Supplier',df_Movement,'Supplier')
df2_random_choice_from_df1(Fac_transaction,'Category',df_Movement,'Category')
random_choice_from_list(df_Movement,'PrimaryContact',PrimaryContact_list_Supplier)
df2_random_choice_from_df1(Fac_transaction,'SupplierReference',df_Movement,'SupplierReference')
df2_random_choice_from_df1(Fac_transaction,'PaymentDays',df_Movement,'PaymentDays')
df2_random_choice_from_df1(Fac_transaction,'PostalCode',df_Movement,'PostalCode')
generate_data(df_Movement,'PaymentMethodKey',1,5)
df2_random_choice_from_df1(Fac_transaction,'PaymentMethodKey',df_Movement,'PaymentMethodKey')

#Customer
df2_random_choice_from_df1(fac_sales,'WWICustomerID',df_Movement,'WWICustomerID')
df2_random_choice_from_df1(fac_sales,'Customer',df_Movement,'Customer')
random_choice_from_list(df_Movement,'BillToCustomer',BillToCustomer_list)
Fac_transaction
df2_random_choice_from_df1(fac_sales,'Category',df_Movement,'Category')
df2_random_choice_from_df1(fac_sales,'BuyingGroup',df_Movement,'BuyingGroup')
df2_random_choice_from_df1(fac_sales,'PrimaryContact',df_Movement,'PrimaryContact')
df2_random_choice_from_df1(fac_sales,'PostalCode',df_Movement,'PostalCode');

# %% [markdown]
# UpdateFact.Purchase

# %%
#Stock_Item
df2_random_choice_from_df1(df_Order,'WWIStockItemID',df_Purchase,'WWIStockItemID')
df2_random_choice_from_df1(df_Order,'StockItem',df_Purchase,'StockItem')
df2_random_choice_from_df1(df_Order,'Color',df_Purchase,'Color')
df2_random_choice_from_df1(df_Order,'SellingPackage',df_Purchase,'SellingPackage')
random_choice_from_list(df_Purchase,'BuyingPackage',Buying_package_list)
random_choice_from_list(df_Purchase,'Brand',Brand_list)
df2_random_choice_from_df1(df_Order,'Size',df_Purchase,'Size')
df2_random_choice_from_df1(df_Order,'LeadTimeDays',df_Purchase,'LeadTimeDays')
df2_random_choice_from_df1(df_Order,'QuantityPerOuter',df_Purchase,'QuantityPerOuter')
df2_random_choice_from_df1(df_Order,'IsChillerStock',df_Purchase,'IsChillerStock')
df2_random_choice_from_df1(df_Order,'Barcode',df_Purchase,'Barcode')
df2_random_choice_from_df1(df_Order,'TaxRate',df_Purchase,'TaxRate')
df2_random_choice_from_df1(df_Order,'UnitPrice',df_Purchase,'UnitPrice')
df2_random_choice_from_df1(df_Order,'RecommendedRetailPrice',df_Purchase,'RecommendedRetailPrice')
df2_random_choice_from_df1(df_Order,'TypicalWeightPerUnit',df_Purchase,'TypicalWeightPerUnit')
df2_random_choice_from_df1(Fac_transaction,'WWISupplierID',df_Purchase,'WWISupplierID')
df2_random_choice_from_df1(Fac_transaction,'Supplier',df_Purchase,'Supplier')
df2_random_choice_from_df1(Fac_transaction,'Category',df_Purchase,'Category')
df2_random_choice_from_df1(Fac_transaction,'PrimaryContact',df_Purchase,'PrimaryContact')
df2_random_choice_from_df1(Fac_transaction,'SupplierReference',df_Purchase,'SupplierReference')
df2_random_choice_from_df1(Fac_transaction,'PaymentDays',df_Purchase,'PaymentDays')
df2_random_choice_from_df1(Fac_transaction,'PostalCode',df_Purchase,'PostalCode')
df2_random_choice_from_df1(Fac_transaction,'PaymentMethodKey',df_Purchase,'PaymentMethodKey');

# %%
#Customer
df2_random_choice_from_df1(fac_sales,'WWICustomerID',df_Order,'WWICustomerID')
df2_random_choice_from_df1(fac_sales,'Customer',df_Order,'Customer')
random_choice_from_list(df_Order,'BillToCustomer',BillToCustomer_list)
df2_random_choice_from_df1(fac_sales,'Category',df_Order,'Category')
df2_random_choice_from_df1(fac_sales,'BuyingGroup',df_Order,'BuyingGroup')
df2_random_choice_from_df1(fac_sales,'PrimaryContact',df_Order,'PrimaryContact')
df2_random_choice_from_df1(fac_sales,'PostalCode',df_Order,'PostalCode');

# %% [markdown]
# ### Update Fact.Sale

# %%
df2_random_choice_from_df1(df_Order,'WWIEmployeeID(Salesperson)',fac_sales,'WWIEmployeeID(Salesperson)')
df2_random_choice_from_df1(df_Order,'Employee(Salesperson)',fac_sales,'Employee(Salesperson)')
df2_random_choice_from_df1(df_Order,'PreferredName(Salesperson)',fac_sales,'PreferredName(Salesperson)')
df2_random_choice_from_df1(df_Order,'WWIStockItemID',fac_sales,'WWIStockItemID')
df2_random_choice_from_df1(df_Order,'StockItem',fac_sales,'StockItem')
df2_random_choice_from_df1(df_Order,'Color',fac_sales,'Color')
df2_random_choice_from_df1(df_Order,'SellingPackage',fac_sales,'SellingPackage')
random_choice_from_list(fac_sales,'BuyingPackage',Buying_package_list)
random_choice_from_list(fac_sales,'Brand',Brand_list)
df2_random_choice_from_df1(df_Order,'Size',fac_sales,'Size')
df2_random_choice_from_df1(df_Order,'LeadTimeDays',fac_sales,'LeadTimeDays')
df2_random_choice_from_df1(df_Order,'QuantityPerOuter',fac_sales,'QuantityPerOuter')
df2_random_choice_from_df1(df_Order,'IsChillerStock',fac_sales,'IsChillerStock')
df2_random_choice_from_df1(df_Order,'Barcode',fac_sales,'Barcode')
df2_random_choice_from_df1(df_Order,'TaxRate',fac_sales,'TaxRate')
df2_random_choice_from_df1(df_Order,'UnitPrice',fac_sales,'UnitPrice')
df2_random_choice_from_df1(df_Order,'RecommendedRetailPrice',fac_sales,'RecommendedRetailPrice')
df2_random_choice_from_df1(df_Order,'TypicalWeightPerUnit',fac_sales,'TypicalWeightPerUnit');

# %%
df_Movement.to_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
df_Order.to_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
df_Purchase.to_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')

# %%
Fac_transaction.to_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
fac_sales.to_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
fac_stockholding.to_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')


