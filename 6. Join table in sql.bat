# %%
import pyodbc
import pandas as pd
import numpy as np
import datetime
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# %%
dbname = "DW"
svname = "DESKTOP-IK0P52V"

# %%
conn_str = ("Driver={SQL Server Native Client 11.0};"
            f"Server={svname};"
            f"Database={dbname};"
            "Trusted_Connection=yes;")
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# %%
general_logging = pd.DataFrame(columns=["FileName","FileType", "Time", "Status"])

# %%
d_customer_new = pd.read_csv(f'./bang_moi/Dim_Customer/Dim_Customer ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
d_stockitem_new = pd.read_csv(f'./bang_moi/Dim_Stock_item/Dim_Stock_item ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
d_supplier_new = pd.read_csv(f'./bang_moi/Dim_Supplier/Dim_Supplier ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')

# %%
f_movement_new = pd.read_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_order_new = pd.read_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_purchase_new = pd.read_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_sale_new = pd.read_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_stockholding_new = pd.read_csv(f'./bang_moi/Fact.Stockholding/Fact.Stockholding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_transaction_new = pd.read_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')

# %% [markdown]
# ### For error testing

# %%
# d_stockitem_new.loc[0,"Color"] = 3
# d_stockitem_new.loc[1,"SellingPackage"] = 3
# d_stockitem_new.loc[2,"BuyingPackage"] = 3
# d_stockitem_new.loc[3,"Brand"] = 3

# d_customer_new.loc[[5,10,12,253],"LineageKey"] = 'a'
# d_customer_new.loc[[1,100,112,153],"Customer"] = np.nan
# d_customer_new.loc[[2,210,212,233],"Customer"] = 3
# d_customer_new.loc[[2,210,212,233],"ValidFrom"] = 'a'
# d_customer_new.loc[[3,300,132,133],"ValidTo"] = 3

# f_movement_new.loc[[12,21,223],"WWIStockItemTransactionID"] = "aa"
# f_movement_new.loc[[123,221,813],"WWIInvoiceID"] = np.nan

# %% [markdown]
# ### Modify datatype

# %%
def structure_date(df,field):
    df[field] = pd.to_datetime(df[field].apply(str), errors = 'ignore')

# %%
# Dim
structure_date(d_customer_new,"ValidFrom")
structure_date(d_customer_new,"ValidTo")

structure_date(d_stockitem_new,"ValidFrom")
structure_date(d_stockitem_new,"ValidTo")

structure_date(d_supplier_new,"ValidFrom")
structure_date(d_supplier_new,"ValidTo")

# Fact
structure_date(f_movement_new,"DateKey")

structure_date(f_order_new,"OrderDateKey")
structure_date(f_order_new,"PickedDateKey")

structure_date(f_purchase_new,"DateKey")

structure_date(f_sale_new,"InvoiceDateKey")
structure_date(f_sale_new,"DeliveryDateKey")

structure_date(f_transaction_new,"DateKey")

# %% [markdown]
# ### Drop FK

# %%
def drop_fk(factname,dimname):
    cursor.execute(f"ALTER TABLE [{dbname}].[dbo].[{factname}]\
   DROP CONSTRAINT [FK_{factname}_{dimname}]")

# %%
drop_fk("Fact.Movement","Dim_Customer")
drop_fk("Fact.Movement","Dim_Date")
drop_fk("Fact.Movement","Dim_Stock_item")
drop_fk("Fact.Movement","Dim_Supplier")
drop_fk("Fact.Movement","Dim_Transaction_type")

drop_fk("Fact.Order","Dim_City")
drop_fk("Fact.Order","Dim_Customer")
drop_fk("Fact.Order","Dim_Date")
drop_fk("Fact.Order","Dim_Date1")
drop_fk("Fact.Order","Dim_Employee")
drop_fk("Fact.Order","Dim_Employee1")
drop_fk("Fact.Order","Dim_Stock_item")

drop_fk("Fact.Purchase","Dim_Date")
drop_fk("Fact.Purchase","Dim_Stock_item")
drop_fk("Fact.Purchase","Dim_Supplier")

drop_fk("Fact.Sale","Dim_City")
drop_fk("Fact.Sale","Dim_Customer")
drop_fk("Fact.Sale","Dim_Date")
drop_fk("Fact.Sale","Dim_Date1")
drop_fk("Fact.Sale","Dim_Employee")
drop_fk("Fact.Sale","Dim_Stock_item")

drop_fk("Fact.StockHolding","Dim_Stock_item")

drop_fk("Fact.Transaction","Dim_Customer")
drop_fk("Fact.Transaction","Dim_Date")
drop_fk("Fact.Transaction","Dim_Payment_method")
drop_fk("Fact.Transaction","Dim_Supplier")
drop_fk("Fact.Transaction","Dim_Transaction_type")

# %% [markdown]
# ### Drop PK

# %%
def drop_pk(dimname):
    cursor.execute(f"ALTER TABLE [{dbname}].[dbo].[{dimname}] DROP CONSTRAINT [PK_{dimname}]")

# %%
drop_pk("Dim_Customer")
drop_pk("Dim_Stock_item")
drop_pk("Dim_Supplier")

# %%
conn.commit()

# %% [markdown]
# ### Dim

# %%
def concat_table(df,df_new):
    result = pd.concat([df, df_new]).drop_duplicates().reset_index(drop=True)
    
    #Remove duplicated hashes
    temp = result['hash'].value_counts()
    if len(temp[temp>1].index.to_list()) == 0:
        pass
    else:
        result_ = pd.DataFrame(columns=result.columns)
        for hash in temp[temp>1].index.to_list():
            result_ = pd.concat([result_,result[result['hash'] == hash].iloc[[0]]]).reset_index(drop=True)
        result = pd.concat([result[~result["hash"].isin(temp[temp>1].index.to_list())],result_]).reset_index(drop=True)
    return result

# %%
def get_df_old(filename):
    sql = f"SELECT * FROM [{dbname}].[dbo].[{filename}]"
    dfold = pd.read_sql(sql,conn)
    return dfold

# %%
def join_table(df,df_new,key):  
    result = concat_table(df,df_new)
    lst = []
    hash_lst = []

    col_list = []
    for col in df_new.columns:
        type_list = []
        for value in df_new[col]:
            if value == 'nan':
                value = np.nan
            if value is not np.nan:
                type_list.append(type(pd.to_datetime(value,errors='ignore',format="%Y-%m-%d")))
        if len(dict(Counter(type_list))) > 1:
            for i in range(len(dict(Counter(type_list)))):
                if list(dict(sorted(dict(Counter(type_list)).items(), key=lambda item: item[1],reverse = True)).values())[i] < 10:
                    col_list.append(col)

    if len(col_list) == 0:
        my_list = list(result[key])
        duplicate_list = list(set([x for x in my_list if my_list.count(x) > 1]))
        for i in range(len(duplicate_list)):
            temp = pd.DataFrame()
            temp = result[result[key] == duplicate_list[i]].reset_index(drop=True)
            if len(temp[temp.Current == 1]) == 2:
                temp1 = temp[temp.Current == 1].reset_index(drop=True)
                temp1.loc[0,'ValidTo'] = temp1.loc[1, 'ValidFrom']
                temp1.loc[0,'Current'] = 0
                temp0 = temp[temp.Current == 0].reset_index(drop=True)
                temp = pd.concat([temp0,temp1])
            
            result = result[result[key] != duplicate_list[i]]
            frames = [result, temp]
            result = pd.concat(frames)
    else:
        # Remove rows with wrong datatype
        for col in col_list:
            type_list = []
            for idx in range(len(df_new)):
                if df_new.loc[idx,col] == 'nan':
                    df_new.loc[idx,col] = np.nan
                if df_new.loc[idx,col] is not np.nan:
                    type_list.append(type(pd.to_datetime(df_new.loc[idx,col],errors='ignore',format="%Y-%m-%d")))
            
            for idx in range(len(df_new)):
                if (type(pd.to_datetime(df_new.loc[idx,col],errors='ignore',format="%Y-%m-%d")) != max(dict(Counter(type_list)), key=dict(Counter(type_list)).get)) & (df_new.loc[idx,col] is not np.nan):
                    lst.append(idx)
        hash_lst = df_new.loc[lst,'hash'].to_list()
        df_new = df_new[~df_new.index.isin(lst)]
        result = concat_table(df,df_new)

        my_list = list(result[key])
        duplicate_list = list(set([x for x in my_list if my_list.count(x) > 1]))
        for i in range(len(duplicate_list)):
            temp = pd.DataFrame()
            temp = result[result[key] == duplicate_list[i]].reset_index(drop=True)
            if len(temp[temp.Current == 1]) == 2:
                temp1 = temp[temp.Current == 1].reset_index(drop=True)
                temp1.loc[0,'ValidTo'] = temp1.loc[1, 'ValidFrom']
                temp1.loc[0,'Current'] = 0
                temp0 = temp[temp.Current == 0].reset_index(drop=True)
                temp = pd.concat([temp0,temp1])
            
            result = result[result[key] != duplicate_list[i]]
            frames = [result, temp]
            result = pd.concat(frames)
    return result.sort_values([key,"Current","ValidFrom"]).reset_index(drop=True), hash_lst

# %%
def dim(filename, dfold, dfnew, key):
    dfold=join_table(dfold,dfnew,key)[0]
    
    col = []
    for ele in dfold.columns:
        if ele in ["hash","Current"]:
            ele = f"[{ele}]"
        col.append(ele)
    col_name = ", ".join(col)

    cursor.execute(f"DELETE FROM [{dbname}].[dbo].[{filename}]")
    conn.commit()

    idx_hash = []
    status_list = []
    for i in range(len(join_table(dfold,dfnew,key)[1])):
        status_list.append("('22018', '[22018] [Microsoft][SQL Server Native Client 11.0][SQL Server]Conversion failed when converting data type. (245) (SQLExecDirectW)')")
    for i in range(len(dfold)):
        try:
            values = []
            for ele in dfold.iloc[i].to_list():
                if ele == 'nan':
                    ele = np.nan
                if (type(ele) in [np.int64, np.float64]) & (ele is not np.nan):
                    values.append(ele)
                elif ele is np.nan:
                    values.append(ele)
                else:
                    ele = f"'{ele}'"
                    values.append(ele)
            values_ = ",".join(map(str,values))
            values_ = values_.replace(',nan,',',NULL,')
            values_ = values_.replace('nan,','NULL,')
            values_ = values_.replace(',nan',',NULL')
            cursor.execute(f"Insert into [{dbname}].[dbo].[{filename}] ({col_name}) values ({values_})")
            status_list.append("Success")
        except Exception as e: 
            status_list.append(e)
            idx_hash.append(i)
    hash_lst = []
    hash_lst = dfold.loc[idx_hash,'hash'].to_list()
    hash_lst = [*hash_lst,*join_table(dfold,dfnew,key)[1]]
    conn.commit()

    return status_list, hash_lst

# %%
d_customer = get_df_old("Dim_Customer")
d_stockitem = get_df_old("Dim_Stock_item")
d_supplier = get_df_old("Dim_Supplier")

# %%
d_customer_status, d_customer_hash = dim("Dim_Customer",d_customer,d_customer_new,'CustomerKey')
d_customer_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

d_stock_item_status, d_stock_item_hash = dim("Dim_Stock_item",d_stockitem,d_stockitem_new,'StockItemKey')
d_stock_item_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

d_supplier_status, d_supplier_hash = dim("Dim_Supplier",d_supplier,d_supplier_new,'SupplierKey')
d_supplier_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

# %%
def get_status(status_list):
    lst = []
    status = "Success"
    for idx, x in enumerate(status_list):
        if x != "Success":
            lst.append(idx)
            status = str(x).replace("\"","'")[:200]
    line = ""
    if len(lst) == 0:
        pass
    elif len(lst) == 1:
        line = "1 line"
    else:
        line = f"{len(lst)} lines"
    return line, status

# %%
#Dim_Customer
if get_status(d_customer_status)[1] == "Success":
    general_logging.loc[-1] = [f'Dim_Customer ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_customer_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Dim_Customer ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_customer_time,\
        f"Fail (Omitted {get_status(d_customer_status)[0]}): {get_status(d_customer_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Dim_Stock_item
if get_status(d_stock_item_status)[1] == "Success":
    general_logging.loc[-1] = [f'Dim_Stock_item ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_stock_item_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Dim_Stock_item ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_stock_item_time,\
        f"Fail (Omitted {get_status(d_stock_item_status)[0]}): {get_status(d_stock_item_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Dim_Supplier
if get_status(d_supplier_status)[1] == "Success":
    general_logging.loc[-1] = [f'Dim_Supplier ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_supplier_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Dim_Supplier ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', d_supplier_time,\
        f"Fail (Omitted {get_status(d_supplier_status)[0]}): {get_status(d_supplier_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

# %% [markdown]
# #### Dim Date

# %%
sql = f"SELECT * FROM [{dbname}].[dbo].[Dim_Date]"
d_date = pd.read_sql(sql,conn)
if d_date["Date"].max() < datetime.date.today():
    col_name = ", ".join(d_date.columns)
    
    sdate = datetime.date(datetime.date.today().year,1,1)
    edate = datetime.date(datetime.date.today().year,12,31)
    d_date_new = pd.DataFrame(pd.date_range(sdate,edate,freq='d'), columns=["Date"])
    d_date_new["DayNumber"] = d_date_new["Date"].dt.day
    d_date_new["Day"] = d_date_new["Date"].dt.day.astype(str)
    d_date_new["Month"] = d_date_new["Date"].dt.strftime('%B')
    d_date_new["ShortMonth"] = d_date_new["Date"].dt.strftime('%b')
    d_date_new["CalendarMonthNumber"] = d_date_new["Date"].dt.month
    d_date_new["CalendarMonthLabel"] = d_date_new["Date"].dt.strftime('CY%Y-%b')
    d_date_new["CalendarYear"] = d_date_new["Date"].dt.year
    d_date_new["CalendarYearLabel"] = d_date_new["Date"].dt.strftime('CY%Y')
    d_date_new["FiscalDay"] = d_date_new["Date"] + pd.DateOffset(months=2)
    d_date_new["FiscalMonthNumber"] = d_date_new["FiscalDay"].dt.month
    d_date_new["FiscalYear"] = d_date_new["FiscalDay"].dt.year
    d_date_new["FiscalYearLabel"] = d_date_new["FiscalDay"].dt.strftime('FY%Y')
    d_date_new["FiscalMonthLabel"] = d_date_new["FiscalYearLabel"] + "-" + d_date_new["ShortMonth"]
    d_date_new["ISOWeekNumber"] = d_date_new["Date"].dt.strftime('%W').astype(int) + 1
    d_date_new.drop(["FiscalDay"],axis=1,inplace=True)
    
    for i in range(len(d_date_new)):
        values = []
        for ele in d_date_new.iloc[i].to_list():
            if type(ele) in [np.int64, np.float64]:
                values.append(ele)
            else:
                ele = f"'{ele}'"
                values.append(ele)
        values_ = ", ".join(map(str,values))
    
        cursor.execute(f"Insert into [{dbname}].[dbo].[Dim_Date] ({col_name})\
            values ({values_})")
    
    general_logging.loc[-1] = [f'Dim_Date ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Dim', datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S"),"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

# %% [markdown]
# ### Fact

# %%
f_movement_new = f_movement_new[~f_movement_new["CustomerHash"].isin(d_customer_hash)]
f_movement_new = f_movement_new[~f_movement_new["StockItemHash"].isin(d_stock_item_hash)]
f_movement_new = f_movement_new[~f_movement_new["SupplierHash"].isin(d_supplier_hash)]

f_order_new = f_order_new[~f_order_new["CustomerHash"].isin(d_customer_hash)]
f_order_new = f_order_new[~f_order_new["StockItemHash"].isin(d_stock_item_hash)]

f_purchase_new = f_purchase_new[~f_purchase_new["StockItemHash"].isin(d_stock_item_hash)]
f_purchase_new = f_purchase_new[~f_purchase_new["SupplierHash"].isin(d_supplier_hash)]

f_sale_new = f_sale_new[~f_sale_new["CustomerHash"].isin(d_customer_hash)]
f_sale_new = f_sale_new[~f_sale_new["StockItemHash"].isin(d_stock_item_hash)]

f_stockholding_new = f_stockholding_new[~f_stockholding_new["StockItemHash"].isin(d_stock_item_hash)]

f_transaction_new = f_transaction_new[~f_transaction_new["CustomerHash"].isin(d_customer_hash)]
f_transaction_new = f_transaction_new[~f_transaction_new["SupplierHash"].isin(d_supplier_hash)]

# %%
f_movement = get_df_old("Fact.Movement")
f_order = get_df_old("Fact.Order")
f_purchase = get_df_old("Fact.Purchase")
f_sale = get_df_old("Fact.Sale")
f_stockholding = get_df_old("Fact.StockHolding")
f_transaction = get_df_old("Fact.Transaction")

# %%
def fact(filename, dfold, dfnew):
    col_name = ", ".join(dfold.columns)

    status_list = []
    for i in range(len(dfnew)):
        try:
            values = []
            for ele in dfnew.iloc[i].to_list():
                if ele == 'nan':
                    ele = np.nan
                if (type(ele) in [np.int64, np.float64]) & (ele is not np.nan):
                    values.append(ele)
                elif ele is np.nan:
                    values.append(ele)
                else:
                    ele = f"'{ele}'"
                    values.append(ele)
            values_ = ",".join(map(str,values))
            values_ = values_.replace(',nan,',',NULL,')
            values_ = values_.replace('nan,','NULL,')
            values_ = values_.replace(',nan',',NULL')
            cursor.execute(f"Insert into [{dbname}].[dbo].[{filename}] ({col_name})\
                values ({values_})")
            status_list.append("Success")
        except Exception as e: 
            status_list.append(e)
    conn.commit()
    return status_list

# %%
f_movement_status = fact("Fact.Movement",f_movement,f_movement_new)
f_movement_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

f_order_status = fact("Fact.Order",f_order,f_order_new)
f_order_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

f_purchase_status = fact("Fact.Purchase",f_purchase,f_purchase_new)
f_purchase_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

f_sale_status = fact("Fact.Sale",f_sale,f_sale_new)
f_sale_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

f_stockholding_status = fact("Fact.StockHolding",f_stockholding,f_stockholding_new)
f_stockholding_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

f_transaction_status = fact("Fact.Transaction",f_transaction,f_transaction_new)
f_transaction_time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

# %%
#Fact.Movement
if get_status(f_movement_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_movement_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_movement_time,\
        f"Fail (Omitted {get_status(f_movement_status)[0]}): {get_status(f_movement_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Fact.Order
if get_status(f_order_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_order_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_order_time,\
        f"Fail (Omitted {get_status(f_order_status)[0]}): {get_status(f_order_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Fact.Purchase
if get_status(f_purchase_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_purchase_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_purchase_time,\
        f"Fail (Omitted {get_status(f_purchase_status)[0]}): {get_status(f_purchase_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Fact.Sale
if get_status(f_sale_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_sale_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_sale_time,\
        f"Fail (Omitted {get_status(f_sale_status)[0]}): {get_status(f_sale_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Fact.StockHolding
if get_status(f_stockholding_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_stockholding_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_stockholding_time,\
        f"Fail (Omitted {get_status(f_stockholding_status)[0]}): {get_status(f_stockholding_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

#Fact.Transaction
if get_status(f_transaction_status)[1] == "Success":
    general_logging.loc[-1] = [f'Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_transaction_time,"Success"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()
else:
    general_logging.loc[-1] = [f'Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv', 'Fact', f_transaction_time,\
        f"Fail (Omitted {get_status(f_transaction_status)[0]}): {get_status(f_transaction_status)[1]}"]
    general_logging.index = general_logging.index + 1
    general_logging = general_logging.sort_index()

# %%
sql = f"SELECT * FROM [{dbname}].[dbo].[GeneralLogging]"
dfold = pd.read_sql(sql,conn)
col_name = ", ".join(dfold.columns)

general_logging["Status"] = general_logging["Status"].str.replace("'","")
for i in range(len(general_logging)):
    values = []
    for ele in general_logging.iloc[i].to_list():
        if type(ele) in [np.int64, np.float64]:
            values.append(ele)
        else:
            ele = f"'{ele}'"
            values.append(ele)
    values_ = ", ".join(map(str,values))

    cursor.execute(f"Insert into [{dbname}].[dbo].[GeneralLogging] ({col_name})\
        values ({values_})")

# %%
def create_fk(factname,dimname,alt_dimname,hashname):
   cursor.execute(f"ALTER TABLE [{dbname}].[dbo].[{factname}]\
      ADD CONSTRAINT [FK_{factname}_{alt_dimname}]\
      FOREIGN KEY ({hashname}) REFERENCES [{dbname}].[dbo].[{dimname}]([hash])")

def create_date_fk(factname,dimname,alt_dimname,hashname):
   cursor.execute(f"ALTER TABLE [{dbname}].[dbo].[{factname}]\
      ADD CONSTRAINT [FK_{factname}_{alt_dimname}]\
      FOREIGN KEY ({hashname}) REFERENCES [{dbname}].[dbo].[{dimname}](Date)")

# %%
def create_pk(dimname):
   cursor.execute(f"ALTER TABLE [{dbname}].[dbo].[{dimname}] ADD CONSTRAINT [PK_{dimname}]  PRIMARY KEY ([hash])")

# %%
create_pk("Dim_Customer")
create_pk("Dim_Stock_item")
create_pk("Dim_Supplier")

# %%
conn.commit()

# %%
create_fk("Fact.Movement","Dim_Customer","Dim_Customer","CustomerHash")
create_date_fk("Fact.Movement","Dim_Date","Dim_Date","DateKey")
create_fk("Fact.Movement","Dim_Stock_item","Dim_Stock_item","StockItemHash")
create_fk("Fact.Movement","Dim_Supplier","Dim_Supplier","SupplierHash")
create_fk("Fact.Movement","Dim_Transaction_type","Dim_Transaction_type","TransactionTypeHash")

create_fk("Fact.Order","Dim_City","Dim_City","CityHash")
create_fk("Fact.Order","Dim_Customer","Dim_Customer","CustomerHash")
create_date_fk("Fact.Order","Dim_Date","Dim_Date","OrderDateKey")
create_date_fk("Fact.Order","Dim_Date","Dim_Date1","PickedDateKey")
create_fk("Fact.Order","Dim_Employee","Dim_Employee","SalespersonHash")
create_fk("Fact.Order","Dim_Employee","Dim_Employee1","PickerHash")
create_fk("Fact.Order","Dim_Stock_item","Dim_Stock_item","StockItemHash")

create_date_fk("Fact.Purchase","Dim_Date","Dim_Date","DateKey")
create_fk("Fact.Purchase","Dim_Stock_item","Dim_Stock_item","StockItemHash")
create_fk("Fact.Purchase","Dim_Supplier","Dim_Supplier","SupplierHash")

create_fk("Fact.Sale","Dim_City","Dim_City","CityHash")
create_fk("Fact.Sale","Dim_Customer","Dim_Customer","CustomerHash")
create_date_fk("Fact.Sale","Dim_Date","Dim_Date","InvoiceDateKey")
create_date_fk("Fact.Sale","Dim_Date","Dim_Date1","DeliveryDateKey")
create_fk("Fact.Sale","Dim_Employee","Dim_Employee","SalespersonHash")
create_fk("Fact.Sale","Dim_Stock_item","Dim_Stock_item","StockItemHash")

create_fk("Fact.StockHolding","Dim_Stock_item","Dim_Stock_item","StockItemHash")

create_fk("Fact.Transaction","Dim_Customer","Dim_Customer","CustomerHash")
create_date_fk("Fact.Transaction","Dim_Date","Dim_Date","DateKey")
create_fk("Fact.Transaction","Dim_Payment_method","Dim_Payment_method","PaymentMethodHash")
create_fk("Fact.Transaction","Dim_Supplier","Dim_Supplier","SupplierHash")
create_fk("Fact.Transaction","Dim_Transaction_type","Dim_Transaction_type","TransactionTypeHash")

# %%
conn.commit()

# %%
conn.close()


