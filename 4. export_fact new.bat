# %%
import numpy as np
import pandas as pd
import random
import time
import string

import datetime

# %%
Fact_Movement = pd.read_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Order = pd.read_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Purchase = pd.read_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Sale = pd.read_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Transaction = pd.read_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_StockHolding = pd.read_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv')

# %%
Fact_Movement = Fact_Movement.loc[:,['MovementKey','DateKey','StockItemKey','CustomerKey',
'SupplierKey','TransactionTypeKey','WWIStockItemTransactionID',
'WWIInvoiceID','WWIPurchaseOrderID','Quantity','LineageKey']]

# %%
Fact_Order = Fact_Order.loc[:,['OrderKey','CityKey','CustomerKey','StockItemKey','OrderDateKey',
'PickedDateKey','SalespersonKey','PickerKey','WWIOrderID','WWIBackorderID',
'Description','Package','UnitPrice','TaxRate','TotalExcludingTax','TaxAmount',
'TotalIncludingTax','LineageKey']]

# %%
Fact_Purchase = Fact_Purchase.loc[:,['PurchaseKey','DateKey','SupplierKey','StockItemKey',
'WWIPurchaseOrderID','OrderedOuters','OrderedQuantity','ReceivedOuters','Package',
'IsOrderFinalized','LineageKey']]

# %%
Fact_Sale = Fact_Sale.loc[:,['SaleKey',
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
'TotalChillerItems', "LineageKey"]]

# %%
Fact_Transaction = Fact_Transaction.loc[:,['TransactionKey',
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
'IsFinalized', "LineageKey"]]

# %%
Fact_StockHolding = Fact_StockHolding.loc[:,['StockHoldingKey',
'StockItemKey',
'QuantityOnHand',
'BinLocation',
'LastStocktakeQuantity',
'LastCostPrice',
'ReorderLevel',
'TargetStockLevel',
'LineageKey']]

# %%
Fact_Movement.to_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_Order.to_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_Purchase.to_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_Sale.to_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_Transaction.to_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)
Fact_StockHolding.to_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.datetime.now().strftime("%m-%d-%Y")}).csv',index=False)


