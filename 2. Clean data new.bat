# %%
import pandas as pd
import numpy as np
import re
import datetime
 
from datetime import timedelta
from datetime import datetime

# %%
Fact_Movement = pd.read_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Order = pd.read_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Purchase = pd.read_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Sale = pd.read_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Transaction = pd.read_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_StockHolding = pd.read_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.now().strftime("%m-%d-%Y")}).csv')

# %%
def structure_date(df,field):
    return pd.to_datetime(df[field], errors = 'coerce')

# %% [markdown]
# ### Fact_Movement

# %%
Fact_Movement['DateKey'] = structure_date(Fact_Movement,'DateKey')
Fact_Movement['ValidFrom'] = structure_date(Fact_Movement,'ValidFrom')
Fact_Movement['ValidTo'] = structure_date(Fact_Movement,'ValidTo')

# %% [markdown]
# ### Fact_Order

# %%
def fix_cell(x):
    x = x.replace("_","-")
    return re.sub(r'[^\w]', ' ', x)
#split Package
Fact_Order['Package'] = Fact_Order['Package'].apply(fix_cell)
Fact_Order[["Quantity", "Package"]] = Fact_Order["Package"].squeeze().str.split(" ",expand = True)

# %% [markdown]
# ####  Structure Date

# %%
Fact_Order['ValidFrom'] = structure_date(Fact_Order,'ValidFrom')
Fact_Order['ValidTo'] = structure_date(Fact_Order,'ValidTo')

Fact_Order['OrderDateKey'] = structure_date(Fact_Order,'OrderDateKey')
Fact_Order['PickedDateKey'] = structure_date(Fact_Order,'PickedDateKey')

# %% [markdown]
# ### Fact_Purchase

# %%
Fact_Purchase['DateKey'] = structure_date(Fact_Purchase,'DateKey')
Fact_Purchase['ValidFrom'] = structure_date(Fact_Purchase,'ValidFrom')
Fact_Purchase['ValidTo'] = structure_date(Fact_Purchase,'ValidTo')

# %% [markdown]
# ### Fact_Sale

# %%
#split Package
Fact_Sale['Package'] = Fact_Sale['Package'].apply(fix_cell)
Fact_Sale[["Quantity", "Package"]] = Fact_Sale["Package"].squeeze().str.split(" ",expand = True)

# %%
Fact_Sale['InvoiceDateKey'] = structure_date(Fact_Sale,'InvoiceDateKey')
Fact_Sale['DeliveryDateKey'] = structure_date(Fact_Sale,'DeliveryDateKey')
Fact_Sale['ValidFrom'] = structure_date(Fact_Sale,'ValidFrom')
Fact_Sale['ValidTo'] = structure_date(Fact_Sale,'ValidTo')

# %% [markdown]
# ### Fact_Transaction

# %%
Fact_Transaction['DateKey'] = structure_date(Fact_Transaction,'DateKey')
Fact_Transaction['ValidFrom'] = structure_date(Fact_Transaction,'ValidFrom')
Fact_Transaction['ValidTo'] = structure_date(Fact_Transaction,'ValidTo')

# %% [markdown]
# ### Fact_StockHolding

# %%
Fact_StockHolding['ValidFrom'] = structure_date(Fact_StockHolding,'ValidFrom')
Fact_StockHolding['ValidTo'] = structure_date(Fact_StockHolding,'ValidTo')

# %%
Fact_Movement.to_csv(f'./bang_moi/Fact.Movement/Fact.Movement ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Order.to_csv(f'./bang_moi/Fact.Order/Fact.Order ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Purchase.to_csv(f'./bang_moi/Fact.Purchase/Fact.Purchase ({datetime.now().strftime("%m-%d-%Y")}).csv')

Fact_Sale.to_csv(f'./bang_moi/Fact.Sale/Fact.Sale ({datetime.now().strftime("%m-%d-%Y")}).csv')
Fact_Transaction.to_csv(f'./bang_moi/Fact.Transaction/Fact.Transaction ({datetime.now().strftime("%m-%d-%Y")}).csv')

Fact_StockHolding.to_csv(f'./bang_moi/Fact.StockHolding/Fact.StockHolding ({datetime.now().strftime("%m-%d-%Y")}).csv')


