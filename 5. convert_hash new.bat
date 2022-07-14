# %%
import pandas as pd
import numpy as np
import datetime

# %%
d_city = pd.read_csv(f'./bang_goc/Dim_City.csv')
d_employee = pd.read_csv(f'./bang_goc/Dim_Employee.csv')
d_paymentmethod = pd.read_csv(f'./bang_goc/Dim_Payment_method.csv')
d_transactiontype = pd.read_csv(f'./bang_goc/Dim_Transaction_type.csv')

d_customer = pd.read_csv(f'./bang_moi/Dim_Customer/Dim_Customer ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
d_stockitem = pd.read_csv(f'./bang_moi/Dim_Stock_item/Dim_Stock_item ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
d_supplier = pd.read_csv(f'./bang_moi/Dim_Supplier/Dim_Supplier ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')


Fact_Movement = pd.read_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Order = pd.read_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_purchase = pd.read_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_sale = pd.read_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_StockHolding = pd.read_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
f_Transaction = pd.read_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')


# %%
f_purchase = f_purchase.merge(d_supplier[["SupplierKey","hash"]],"left",on="SupplierKey").rename({"hash":"SupplierHash"},axis=1).drop("SupplierKey",axis=1)
f_purchase = f_purchase.merge(d_stockitem[["StockItemKey","hash"]],"left",on="StockItemKey").rename({"hash":"StockItemHash"},axis=1).drop("StockItemKey",axis=1)

# %%
f_sale = f_sale.merge(d_city[["CityKey","hash"]],"left",on="CityKey").rename({"hash":"CityHash"},axis=1).drop("CityKey",axis=1)
f_sale = f_sale.merge(d_customer[["CustomerKey","hash"]],"left",on="CustomerKey").rename({"hash":"CustomerHash"},axis=1).drop("CustomerKey",axis=1)
f_sale = f_sale.merge(d_stockitem[["StockItemKey","hash"]],"left",on="StockItemKey").rename({"hash":"StockItemHash"},axis=1).drop("StockItemKey",axis=1)
f_sale = f_sale.merge(d_employee[["EmployeeKey","hash"]],"left",left_on="SalespersonKey",right_on="EmployeeKey").rename({"hash":"SalespersonHash"},axis=1).drop(["SalespersonKey","EmployeeKey"],axis=1)

# %%
temp = d_stockitem[['StockItemKey','hash']]

Fact_Movement = pd.merge(Fact_Movement, temp, on='StockItemKey', how='left')
Fact_Movement = Fact_Movement.drop('StockItemKey',axis=1)
Fact_Movement = Fact_Movement.rename(columns = {'hash':'StockItemHash'})

# %%
temp = d_customer[['CustomerKey','hash']]

Fact_Movement = pd.merge(Fact_Movement, temp, on='CustomerKey', how='left')
Fact_Movement = Fact_Movement.drop('CustomerKey',axis=1)
Fact_Movement = Fact_Movement.rename(columns = {'hash':'CustomerHash'})

# %%
temp = d_supplier[['SupplierKey','hash']]

Fact_Movement = pd.merge(Fact_Movement, temp, on='SupplierKey', how='left')
Fact_Movement = Fact_Movement.drop('SupplierKey',axis=1)
Fact_Movement = Fact_Movement.rename(columns = {'hash':'SupplierHash'})


# %%
temp = d_transactiontype[['TransactionTypeKey','hash']]

Fact_Movement = pd.merge(Fact_Movement, temp, on='TransactionTypeKey', how='left')
Fact_Movement = Fact_Movement.drop('TransactionTypeKey',axis=1)
Fact_Movement = Fact_Movement.rename(columns = {'hash':'TransactionTypeHash'})

# %%
temp = d_city[['CityKey','hash']]

Fact_Order = pd.merge(Fact_Order, temp, on='CityKey', how='left')
Fact_Order = Fact_Order.drop('CityKey',axis=1)
Fact_Order = Fact_Order.rename(columns = {'hash':'CityHash'})

# %%
temp = d_customer[['CustomerKey','hash']]

Fact_Order = pd.merge(Fact_Order, temp, on='CustomerKey', how='left')
Fact_Order = Fact_Order.drop('CustomerKey',axis=1)
Fact_Order = Fact_Order.rename(columns = {'hash':'CustomerHash'})

# %%
temp = d_stockitem[['StockItemKey','hash']]

Fact_Order = pd.merge(Fact_Order, temp, on='StockItemKey', how='left')
Fact_Order = Fact_Order.drop('StockItemKey',axis=1)
Fact_Order = Fact_Order.rename(columns = {'hash':'StockItemHash'})

# %%
temp = d_employee[['EmployeeKey','hash']]

Fact_Order =pd.merge(Fact_Order, temp, left_on='SalespersonKey', right_on='EmployeeKey')
Fact_Order = Fact_Order.drop(['SalespersonKey','EmployeeKey'],axis=1)
Fact_Order = Fact_Order.rename(columns = {'hash':'SalespersonHash'})

# %%
temp = d_employee[['EmployeeKey','hash']]

Fact_Order =pd.merge(Fact_Order, temp, left_on='PickerKey', right_on='EmployeeKey')
Fact_Order = Fact_Order.drop(['PickerKey','EmployeeKey'],axis=1)
Fact_Order = Fact_Order.rename(columns = {'hash':'PickerHash'})

# %%
# f_StockHolding
f_StockHolding = f_StockHolding.merge(d_stockitem[['StockItemKey','hash']],"left",on="StockItemKey").rename({"hash":"StockItemHash"},axis=1).drop("StockItemKey",axis=1)

# %%
# f_Transaction
f_Transaction = f_Transaction.merge(d_supplier[["SupplierKey","hash"]],"left",on="SupplierKey").rename({"hash":"SupplierHash"},axis=1).drop("SupplierKey",axis=1)
f_Transaction = f_Transaction.merge(d_paymentmethod[["PaymentMethodKey","hash"]],"left",on="PaymentMethodKey").rename({"hash":"PaymentMethodHash"},axis=1).drop("PaymentMethodKey",axis=1)
f_Transaction = f_Transaction.merge(d_transactiontype[["TransactionTypeKey","hash"]],"left",on="TransactionTypeKey").rename({"hash":"TransactionTypeHash"},axis=1).drop("TransactionTypeKey",axis=1)
f_Transaction = f_Transaction.merge(d_customer[["CustomerKey","hash"]],"left",on="CustomerKey").rename({"hash":"CustomerHash"},axis=1).drop("CustomerKey",axis=1)

# %%
Fact_Movement.to_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_Order.to_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
f_purchase.to_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
f_sale.to_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
f_Transaction.to_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
f_StockHolding.to_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)


