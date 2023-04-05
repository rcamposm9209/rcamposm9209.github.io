---
layout: post
title: "Control tower"
subtitle: "Automatization of control tower to supply"
background: '/img/posts/web-scraping/back-ground-ws.jpg'
---

# Control Tower


```python
#!pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```


```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import snowflake.connector
import requests
import io
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
```


```python
conn = snowflake.connector.connect(user='', 
                                   authenticator='', 
                                   account='', 
                                   warehouse="",
                                   database="")
```

## Descarga WL


```python
wl="""SELECT city,
warehouseid,
scope,
category_one, 
category_two,
category_three,
storereferenceid, 
product_ean,
name,
maker
FROM fivetran.cpgs_turbo_ds_public.global_wishlist_with_pareto_new 
WHERE country_code = 'CO'
"""
```


```python
df_wl = pd.read_sql(wl,conn)
df_wl.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\4136487182.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_wl = pd.read_sql(wl,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CITY</th>
      <th>WAREHOUSEID</th>
      <th>SCOPE</th>
      <th>CATEGORY_ONE</th>
      <th>CATEGORY_TWO</th>
      <th>CATEGORY_THREE</th>
      <th>STOREREFERENCEID</th>
      <th>PRODUCT_EAN</th>
      <th>NAME</th>
      <th>MAKER</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Bogotá</td>
      <td>286</td>
      <td>BOG ORO</td>
      <td>Despensa y productos secos</td>
      <td>Snacks y confitería</td>
      <td>Gomitas y caramelos</td>
      <td>435192</td>
      <td>7702993041277</td>
      <td>Gomas Gusanos Acidos - Trululu - 1 Und - Trulu...</td>
      <td>Super De Alimentos</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Bogotá</td>
      <td>283</td>
      <td>BOG ORO</td>
      <td>Bebidas</td>
      <td>Cervezas y sidras</td>
      <td>Cervezas</td>
      <td>432487</td>
      <td>83741531460</td>
      <td>CERVEZA SIXPACK BOTEL - Grolsch - 1 ud. - Grol...</td>
      <td>Global Wine &amp; Spirits</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga Inventario


```python
url=''
urlData = requests.get(url).content
inventario = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
```


```python
inventario.drop(['cruce','warehouse_name','ean'],axis=1,inplace=True)
```


```python
inventario.rename(columns={'warehouse_id':'WAREHOUSEID','storereferenceid':'STOREREFERENCEID','stock':'INVENTARIO_TURBO'},
                  inplace=True)
```


```python
#inventario["WAREHOUSEID"]=np.where(inventario["WAREHOUSEID"]==321,309,inventario["WAREHOUSEID"])
#inventario["WAREHOUSEID"]=np.where(inventario["WAREHOUSEID"]==322,310,inventario["WAREHOUSEID"])
```

## Descarga datos tiendas


```python
store = """select w.id AS warehouseid, 
w.supplierwarehouseid AS dependencia, 
w.name AS nombre_tienda,
g.locationid
from fivetran.co_amysql_turbo_vivo_core_api.warehouse as w
left join fivetran.cpgs_turbo_ds_public.global_warehouse as g
on w.id = g.warehouse_id
where w.supplierwarehouseid is not null
and g.country_code = 'CO'
and (integrated_warehouse >= 1 or warehouseid in (321, 322))
"""
```


```python
df_store = pd.read_sql(store, conn)
df_store.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\618108488.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_store = pd.read_sql(store, conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>DEPENDENCIA</th>
      <th>NOMBRE_TIENDA</th>
      <th>LOCATIONID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>261</td>
      <td>4449</td>
      <td>Pasadena</td>
      <td>38</td>
    </tr>
    <tr>
      <th>1</th>
      <td>278</td>
      <td>4488</td>
      <td>Los colores</td>
      <td>7</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga datos SWA


```python
swa = """select a.storereferenceid,  
a.warehouseid, 
g.supplier_name, 
c.wl_type,
c.scope,
case when a.sales_units > 0 then a.sales_units else 0 end as sales_ayer, 
a.full_available_hours, 
a.full_total_hours,
a.available_hours, 
a.total_hours,

(a.full_available_hours / NULLIF(a.full_total_hours,0)) AS avl_total,
(a.available_hours / NULLIF(a.total_hours,0)) AS avl_base,

case when a.full_sales_28 > 0 then a.full_sales_28 else 0 end as full_sales_28,
sales_28

from fivetran.cpgs_turbo_ds_public.global_closing_inventory_current a

left join (select x.country_code, x.warehouse_id, x.warehouse_name, x.physical_store_id, x.integrated_warehouse, x.locationid, y.city_name 
     from fivetran.cpgs_turbo_ds_public.global_warehouse x
     left join fivetran.cpgs_turbo_ds_public.global_divipola y
     on x.country_code = y.country_code and x.locationid = y.location_id
     where x.country_code = 'CO') b
on a.country_code = b.country_code and a.warehouseid = b.warehouse_id

left join fivetran.cpgs_turbo_ds_public.global_wishlist_with_pareto_new c
on a.country_code = c.country_code and a.storereferenceid = c.storereferenceid and a.warehouseid = c.warehouseid

left join (select country, storereferenceid, case when name like concat(concat('%',chr(10)),'%') or name like concat(concat('%',chr(9)),'%') then substr(name, 0, position('\n', name, 1) - 1) else name end as name, macrocategory_name, category_name
    from fivetran.cpgs_turbo_ds_public.global_catalog_datascience
    group by 1,2,3,4,5) d
    on a.country_code = d.country and a.storereferenceid = d.storereferenceid

left join (select e.country_code, e.storereferenceid, e.providerid, f.supplier_name, e.locationid from fivetran.cpgs_turbo_ds_public.global_supplier_ranking e
     left join fivetran.cpgs_turbo_ds_public.global_suppliers f
     on e.country_code = f.country_code and e.providerid = f.supplier_id where e.ranker = 1) g
on a.country_code = g.country_code and a.storereferenceid = g.storereferenceid and b.locationid = g.locationid

--left join fivetran.cpgs_turbo_ds_public.global_r2e h on a.country_code = h.country and a.storereferenceid = h.storereferenceid

having a.tag = 'Regular' and c.wl_type in ('1 Ideal', '3 Substitute') and b.integrated_warehouse >= 1 
/*and h.storereferenceid is null*/ and cast(a.main_date as date) = current_date - interval '1 day' and a.country_code = 'CO'
"""
```


```python
df_swa = pd.read_sql(swa, conn)
df_swa.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\2509653294.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_swa = pd.read_sql(swa, conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STOREREFERENCEID</th>
      <th>WAREHOUSEID</th>
      <th>SUPPLIER_NAME</th>
      <th>WL_TYPE</th>
      <th>SCOPE</th>
      <th>SALES_AYER</th>
      <th>FULL_AVAILABLE_HOURS</th>
      <th>FULL_TOTAL_HOURS</th>
      <th>AVAILABLE_HOURS</th>
      <th>TOTAL_HOURS</th>
      <th>AVL_TOTAL</th>
      <th>AVL_BASE</th>
      <th>FULL_SALES_28</th>
      <th>SALES_28</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>427288</td>
      <td>182</td>
      <td>Almacenes Exito S.A.</td>
      <td>1 Ideal</td>
      <td>BOG DIAMANTE</td>
      <td>0.0</td>
      <td>23</td>
      <td>23</td>
      <td>23</td>
      <td>23</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>18.0</td>
      <td>18.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>431581</td>
      <td>244</td>
      <td>Almacenes Exito S.A.</td>
      <td>1 Ideal</td>
      <td>COSTA DIAMANTE</td>
      <td>0.0</td>
      <td>23</td>
      <td>23</td>
      <td>23</td>
      <td>23</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>30.0</td>
      <td>30.0</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga forecast - 1


```python
forecast_last="""SELECT warehouse_id as warehouseid,
retail_id as storereferenceid,
SUM(forecast) AS forecast_last
FROM fivetran.cpgs_turbo_ds_public.global_forecast_main
WHERE country = 'CO'
AND date between date_trunc('WEEK', current_date - interval '1 WEEK') AND date_trunc('WEEK', current_date - interval '1 WEEK')+ interval '6 days'
GROUP BY warehouse_id, retail_id
"""
```


```python
df_forecast_last = pd.read_sql(forecast_last,conn)
df_forecast_last.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\1610222774.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_forecast_last = pd.read_sql(forecast_last,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>FORECAST_LAST</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>236</td>
      <td>429751</td>
      <td>34.416667</td>
    </tr>
    <tr>
      <th>1</th>
      <td>230</td>
      <td>431993</td>
      <td>10.500000</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga forecast + 1


```python
forecast_1="""SELECT warehouse_id as warehouseid,
retail_id as storereferenceid,
SUM(forecast) AS forecast1
FROM fivetran.cpgs_turbo_ds_public.global_forecast_main
WHERE country = 'CO'
AND date between date_trunc('WEEK', current_date + interval '1 WEEK') AND date_trunc('WEEK', current_date + interval '1 WEEK')+ interval '6 days'
GROUP BY warehouse_id, retail_id
"""
```


```python
df_forecast_1 = pd.read_sql(forecast_1,conn)
df_forecast_1.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\2230000663.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_forecast_1 = pd.read_sql(forecast_1,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>FORECAST1</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>239</td>
      <td>437612</td>
      <td>2.45</td>
    </tr>
    <tr>
      <th>1</th>
      <td>265</td>
      <td>429173</td>
      <td>0.75</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga forecast + 2


```python
forecast_2="""SELECT warehouse_id as warehouseid,
retail_id as storereferenceid,
SUM(forecast) AS forecast2
FROM fivetran.cpgs_turbo_ds_public.global_forecast_main
WHERE country = 'CO'
AND date between date_trunc('WEEK', current_date + interval '2 WEEK') AND date_trunc('WEEK', current_date + interval '2 WEEK')+ interval '6 days'
GROUP BY warehouse_id, retail_id
"""
```


```python
df_forecast_2 = pd.read_sql(forecast_2,conn)
df_forecast_2.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\216003158.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_forecast_2 = pd.read_sql(forecast_2,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>FORECAST2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>245</td>
      <td>426990</td>
      <td>139.80</td>
    </tr>
    <tr>
      <th>1</th>
      <td>254</td>
      <td>427241</td>
      <td>1.95</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga productos éxito


```python
plu = """SELECT * 
FROM fivetran.cpgs_turbo_ds_public.co_exito_producto
"""
```


```python
df_plu = pd.read_sql(plu, conn)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\3632562405.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_plu = pd.read_sql(plu, conn)
    


```python
df_plu.rename(columns={"VIVO_ID":"STOREREFERENCEID","Factor":"FACTOR"},inplace=True)
df_plu.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STOREREFERENCEID</th>
      <th>PLU_PADRE</th>
      <th>PLU_HIJO</th>
      <th>FACTOR</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>430678</td>
      <td>394209</td>
      <td>394209</td>
      <td>1,0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>434842</td>
      <td>256676</td>
      <td>256676</td>
      <td>1,0</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga Bucket


```python
buc = """SELECT warehouseid, 
storereferenceid,
bucket
FROM fivetran.cpgs_turbo_ds_public.co_bucket_inventory
WHERE country_code = 'CO'
"""
```


```python
df_bucket = pd.read_sql(buc, conn)
df_bucket.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\2338218854.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_bucket = pd.read_sql(buc, conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>BUCKET</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>277</td>
      <td>435782</td>
      <td>A</td>
    </tr>
    <tr>
      <th>1</th>
      <td>277</td>
      <td>436873</td>
      <td>A</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga última fecha entrada


```python
entrada= """
SELECT f.warehouseid,
CAST(f.storereferenceid AS int) as storereferenceid, 
f.event_date AS fecha_entrada
FROM
(SELECT 'ENTRADA' AS tipo, 
CONVERT_TIMEZONE('UTC','America/Bogota',a.createdat) AS event_date,
a.warehouseid,
w.name,
c.productid,
c.storereferenceid,
c.stock as cantidad, 
ROW_NUMBER() OVER(PARTITION BY CONCAT(c.storereferenceid, a.warehouseid) ORDER BY event_date DESC) AS rn
FROM  FIVETRAN.co_AMYSQL_TURBO_VIVO_CORE_API.Entry a
LEFT JOIN FIVETRAN.co_AMYSQL_TURBO_VIVO_CORE_API.Entrytype AS b ON a.entrytypeid = b.id 
LEFT JOIN FIVETRAN.co_AMYSQL_TURBO_VIVO_CORE_API.Entrydetail AS c ON c.entryid = a.id
LEFT JOIN FIVETRAN.CO_AMYSQL_TURBO_VIVO_CORE_API.Warehouse AS w ON (a.warehouseid = w.id)
WHERE COALESCE(a._fivetran_deleted, 'FALSE') = 'FALSE'
AND COALESCE(b._fivetran_deleted, 'FALSE') = 'FALSE' 
AND COALESCE(c._fivetran_deleted, 'FALSE') = 'FALSE'
--AND (b.name) IN ('Masivo','Conversión')
AND a.deletedat IS NULL
ORDER BY event_date DESC) f
WHERE f.rn IN (1)
AND f.storereferenceid IS NOT NULL
"""
```


```python
df_entrada = pd.read_sql(entrada,conn)
df_entrada.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\977560597.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_entrada = pd.read_sql(entrada,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>FECHA_ENTRADA</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>296</td>
      <td>433444</td>
      <td>2023-04-04 08:35:47</td>
    </tr>
    <tr>
      <th>1</th>
      <td>296</td>
      <td>429185</td>
      <td>2023-04-04 08:35:47</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga DOI


```python
doi = """with DOI as (
select COUNTRY, city_name, WAREHOUSE_ID, RETAIL_ID, STOCK_MONEY_MONTHLY, SALES_MONTHLY, CREATEDAT
from FIVETRAN.CPGS_CARGO.ES_FINANCE_GLOBAL_INVENTORY_HISTORY_LONG
where CREATEDAT = current_date()-2
and country = 'CO'
),

WH as (
select COUNTRY_CODE, WAREHOUSE_ID, WAREHOUSE_NAME, locationid
from FIVETRAN.CPGS_TURBO_DS_PUBLIC.GLOBAL_WAREHOUSE 
where (INTEGRATED_WAREHOUSE in (1,2,3) and WAREHOUSE_NAME not like '%INACT%' and WAREHOUSE_NAME not like '%urboX%' and WAREHOUSE_NAME not like '%urbo X%' and WAREHOUSE_NAME not like '%Turbo by Li%') 
--or WAREHOUSE_NAME in ('CD GUADALAJARA', 'CD Iflow', 'CD Itapevi', 'CD MONTERREY', 'CD Rio de Janeiro', 'CD Traxion')
and country_code = 'CO'
),

DOI_WH as (
select a.* 
from DOI a
left join WH b
on a.COUNTRY = b.COUNTRY_CODE and a.WAREHOUSE_ID = b.WAREHOUSE_ID
having b.WAREHOUSE_NAME is not null
),

catalogo AS 
(SELECT country, 
storereferenceid, 
CASE WHEN name LIKE '%\n%' THEN SUBSTR(name, 0, POSITION('\n', name, 1) - 1) ELSE name END AS name, 
category_name
FROM fivetran.cpgs_turbo_ds_public.global_catalog_datascience
WHERE turbo = 'Turbo' 
AND rp_status = 'published'
AND country = 'CO'),

wh AS (
SELECT country_code, 
warehouse_id AS warehouseid, 
warehouse_name,
locationid
FROM fivetran.cpgs_turbo_ds_public.global_warehouse
WHERE country_code = 'CO'
AND integrated_warehouse IN (1,2,3)  
--OR IS_CEDI = 'TRUE'
),

wl as (select warehouseid, 
storereferenceid, 
wl_type
from fivetran.cpgs_turbo_ds_public.global_wishlist_with_pareto_new
where country_code = 'CO')

select r.warehouse_id as warehouseid, 
retail_id as storereferenceid, 
DOI_global
from (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY warehouse_id, category_name ORDER BY DOI_global DESC) as orden
FROM 
(
select category_name,
name, 
WAREHOUSE_ID, 
retail_id, 
round(sum(STOCK_MONEY_MONTHLY)/NULLIF(sum(SALES_MONTHLY),0),2) as DOI_global,
round(STOCK_MONEY_MONTHLY,2) as STOCK_MONEY_MONTHLY,
round(SALES_MONTHLY,2) as SALES_MONTHLY
from DOI_WH as d
left join catalogo as c
on c.storereferenceid = d.retail_id
where d.country = 'CO'
group by 1,2,3,4,6,7)
WHERE DOI_global is not null
) as r
left join wh as w
on w.WAREHOUSE_ID = r.WAREHOUSE_ID
left join wl as wl
on wl.warehouseid = r.warehouse_id and wl.storereferenceid = r.retail_id
order by 1 asc;
"""
```


```python
df_doi = pd.read_sql(doi, conn)
df_doi.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\1440314510.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_doi = pd.read_sql(doi, conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>DOI_GLOBAL</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>180</td>
      <td>434246</td>
      <td>30.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>180</td>
      <td>426915</td>
      <td>7.01</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga salir WL


```python
wl_left = """SELECT warehouseid, 
storereferenceid,
keep_in_wl
FROM fivetran.cpgs_turbo_ds_public.co_portfolio_out_review
WHERE country_code = 'CO'
AND keep_in_wl <> '-'
AND first_sale > 60
"""
```


```python
df_wl_left = pd.read_sql(wl_left,conn)
df_wl_left.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\937060501.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_wl_left = pd.read_sql(wl_left,conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>STOREREFERENCEID</th>
      <th>KEEP_IN_WL</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>180</td>
      <td>437065</td>
      <td>Se recomienda sacar de la WL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>180</td>
      <td>437070</td>
      <td>Se recomienda sacar de la WL</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga tiene sustituto


```python
sustituto = """WITH
ideales AS
(SELECT warehouseid,
storereferenceid
FROM FIVETRAN.CPGS_TURBO_DS_PUBLIC.GLOBAL_WISHLIST_WITH_PARETO_NEW
WHERE country_code = 'CO'
AND wl_type = '1 Ideal'),

sustitutos AS
(SELECT warehouseid,
parentstorereferenceid,
storereferenceid AS storereferenceid_sustituto
FROM FIVETRAN.CPGS_TURBO_DS_PUBLIC.GLOBAL_WISHLIST_WITH_PARETO_NEW
WHERE country_code = 'CO'
AND wl_type = '3 Substitute')

SELECT DISTINCT i.warehouseid,
s.parentstorereferenceid,
count(distinct s.storereferenceid_sustituto) as num_sustitutos
FROM ideales AS i
LEFT JOIN sustitutos AS s
ON i.warehouseid = s.warehouseid AND i.storereferenceid = s.parentstorereferenceid
WHERE parentstorereferenceid IS NOT NULL
GROUP BY i.warehouseid, s.parentstorereferenceid
"""
```


```python
df_sustituto = pd.read_sql(sustituto, conn)
df_sustituto.head(2)
```

    C:\Users\ricardo.campos\AppData\Local\Temp\ipykernel_16788\3903861253.py:1: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
      df_sustituto = pd.read_sql(sustituto, conn)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>WAREHOUSEID</th>
      <th>PARENTSTOREREFERENCEID</th>
      <th>NUM_SUSTITUTOS</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>262</td>
      <td>430913</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>244</td>
      <td>432685</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga archivo éxito


```python
exito = pd.read_excel("base_exito.xlsx")
```


```python
exito.drop(columns=["Dependencia DESC","Subzona ID","Subzona DESC","Ciudad Cod Dane Ciudad","Ciudad DESC",
                   "Ciudad Cod Dane Ciudad","Ciudad DESC","Plu DESC","Marca DESC","Marca ID","Sublinea ID",
                   "Sublinea DESC","Categoria ID categoria","Categoria DESC","Subcategoria IDsubcategoria","Subcategoria DESC",
                   "Direccion DESC","Clasificaciones ID","Cubrimiento Stock Presentacion","InventarioMaximo","DemDia",
                   "Cubrimiento Inventario","Demsem"], inplace=True)
```


```python
exito.rename(columns={"Dependencia DependenciaCD":"DEPENDENCIA","Ean ID":"EAN","Plu PluCD":"PLU_PADRE",
                      "Proveedor NombreProveedor":"PROVEEDOR_FINAL","Descripcionestadoplu ID":"ESTADO_PLU",
                      "Cediatiende ID":"CEDI","Inventario":"INVENTARIO_EXITO","PedidoPendiente":"PEDIDO_PENDIENTE",
                      "Proveedor Nit":"NIT_PROVEEDOR_EXITO","StockDePresentacion":"STOCK_PRESENTACION","Umd":"UMD"}, inplace=True)
```


```python
exito.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DEPENDENCIA</th>
      <th>EAN</th>
      <th>PLU_PADRE</th>
      <th>NIT_PROVEEDOR_EXITO</th>
      <th>PROVEEDOR_FINAL</th>
      <th>ESTADO_PLU</th>
      <th>CEDI</th>
      <th>INVENTARIO_EXITO</th>
      <th>PEDIDO_PENDIENTE</th>
      <th>STOCK_PRESENTACION</th>
      <th>UMD</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4442</td>
      <td>50196135.0</td>
      <td>1251231</td>
      <td>830006051</td>
      <td>DIAGEO COLOMBIA S.A.</td>
      <td>Activo</td>
      <td>0</td>
      <td>4</td>
      <td>2</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4442</td>
      <td>50196388.0</td>
      <td>24334</td>
      <td>830006051</td>
      <td>DIAGEO COLOMBIA S.A.</td>
      <td>Activo</td>
      <td>0</td>
      <td>11</td>
      <td>6</td>
      <td>4</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga agenda turbo


```python
agenda = pd.read_excel("agenda_turbo.xlsx")
```


```python
agenda.drop(columns=["Dependencia Nombre","Descripcion PLU","Descripcion PLU","Marca","Cedi","Nit","Proveedor Nombre",
                     "Cadena","Cadena Nombre","Zona","Zona Nombre","Subzona Nombre","Estado PLU Sinco","Unidades Pallet"],
            inplace=True)
```


```python
agenda.rename(columns={"Dep":"DEPENDENCIA","PLU":"PLU_PADRE","Order Days in Week":"OC_DAYS","Order Cycle":"LEAD_TIME"},
              inplace=True)
```


```python
agenda.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DEPENDENCIA</th>
      <th>PLU_PADRE</th>
      <th>OC_DAYS</th>
      <th>LEAD_TIME</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4439</td>
      <td>202371</td>
      <td>246.0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4439</td>
      <td>218789</td>
      <td>14.0</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga otif directo


```python
directo_df = pd.read_excel("otif_directo.xlsx")
directo_df.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>dependencia</th>
      <th>plu_padre</th>
      <th>otif_directo</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4442</td>
      <td>12751</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4442</td>
      <td>16093</td>
      <td>0.777778</td>
    </tr>
  </tbody>
</table>
</div>




```python
directo_df.rename(columns={"dependencia":"DEPENDENCIA","plu_padre":"PLU_PADRE","otif_directo":"OTIF_DIRECTO"},inplace=True)
```

## Descarga otif cedi


```python
cedi_df = pd.read_excel("otif_cedi.xlsx")
cedi_df.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>NIT_PROVEEDOR_EXITO</th>
      <th>PLU_PADRE</th>
      <th>OTIF_CEDI</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11786142</td>
      <td>3141869</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>17145642</td>
      <td>651396</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
</div>



## Descarga vida útil


```python
vida_util = pd.read_excel("vida_util.xlsx")
vida_util.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CATEGORY_THREE</th>
      <th>DIAS_VENTA_DISPONIBLE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sushi</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Otras comidas preparadas</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



# UNIÓN DE INFORMACIÓN

### wl - sustituto


```python
df_1 = pd.merge(left=df_wl,right=df_sustituto,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","PARENTSTOREREFERENCEID"])
```


```python
df_1.fillna(0,inplace=True)
```

### wl / sustituto - bucket


```python
df_2 = pd.merge(left=df_1,right=df_bucket,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket - tiendas


```python
df_3 = pd.merge(left=df_2,right=df_store,how='left',left_on=["WAREHOUSEID"],
                         right_on=["WAREHOUSEID"])
```

### wl / sustituto / bucket / tiendas - DOI


```python
df_4 = pd.merge(left=df_3,right=df_doi,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket / tiendas / DOI - inventario


```python
df_5 = pd.merge(left=df_4,right=inventario,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario - salir_wl


```python
df_6 = pd.merge(left=df_5,right=df_wl_left,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl - plu


```python
df_plu["STOREREFERENCEID"]=df_plu["STOREREFERENCEID"].astype(int)
```


```python
df_7 = pd.merge(left=df_6,right=df_plu,how='left',left_on=["STOREREFERENCEID"],
                         right_on=["STOREREFERENCEID"])
```


```python
#Para que pueda convertir a int
df_7["PLU_PADRE"].fillna(0,inplace=True)
```


```python
df_7["PLU_PADRE"]=df_7["PLU_PADRE"].astype(int)
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu - agenda


```python
df_8 = pd.merge(left=df_7,right=agenda,how='left',left_on=["DEPENDENCIA","PLU_PADRE"],
                         right_on=["DEPENDENCIA","PLU_PADRE"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda - otif_directo


```python
df_9 = pd.merge(left=df_8,right=directo_df,how='left',left_on=["DEPENDENCIA","PLU_PADRE"],
                         right_on=["DEPENDENCIA","PLU_PADRE"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo - base_exito


```python
df_10 = pd.merge(left=df_9,right=exito,how='left',left_on=["DEPENDENCIA","PLU_PADRE"],
                         right_on=["DEPENDENCIA","PLU_PADRE"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo / base_exito - otif_cedi


```python
df_11 = pd.merge(left=df_10,right=cedi_df,how='left',left_on=["NIT_PROVEEDOR_EXITO","PLU_PADRE"],
                         right_on=["NIT_PROVEEDOR_EXITO","PLU_PADRE"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo / otif_cedi - swa


```python
df_12 = pd.merge(left=df_11,right=df_swa,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### Forecast


```python
forecast_1 = pd.merge(left=df_forecast_last,right=df_forecast_1,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```


```python
forecast_fin = pd.merge(left=forecast_1,right=df_forecast_2,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo / otif_cedi / swa - forecast


```python
df_13 = pd.merge(left=df_12,right=forecast_fin,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo / otif_cedi / swa / forecast - fecha_entrada


```python
df_14 = pd.merge(left=df_13,right=df_entrada,how='left',left_on=["WAREHOUSEID","STOREREFERENCEID"],
                         right_on=["WAREHOUSEID","STOREREFERENCEID"])
```


```python
# Modificando de acuerdo el parámetro el producto
df_14["FACTOR"]=df_14["FACTOR"].str.replace(",",".")
df_14["FACTOR"]=df_14["FACTOR"].astype("float")
df_14["INVENTARIO_EXITO"] = df_14["INVENTARIO_EXITO"] / df_14["FACTOR"]
df_14["PEDIDO_PENDIENTE"] = df_14["PEDIDO_PENDIENTE"] / df_14["FACTOR"]
df_14["STOCK_PRESENTACION"] = df_14["STOCK_PRESENTACION"] / df_14["FACTOR"]
df_14["UMD"] = df_14["UMD"] / df_14["FACTOR"]
```

### wl / sustituto / bucket / tiendas / DOI / inventario / salir_wl / plu / agenda / otif_directo / otif_cedi / swa / forecast / fecha_entrada - vida_util


```python
df_15 = pd.merge(left=df_14,right=vida_util,how='left',left_on=["CATEGORY_THREE"],right_on=["CATEGORY_THREE"])
df_15.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CITY</th>
      <th>WAREHOUSEID</th>
      <th>SCOPE_x</th>
      <th>CATEGORY_ONE</th>
      <th>CATEGORY_TWO</th>
      <th>CATEGORY_THREE</th>
      <th>STOREREFERENCEID</th>
      <th>PRODUCT_EAN</th>
      <th>NAME</th>
      <th>MAKER</th>
      <th>...</th>
      <th>TOTAL_HOURS</th>
      <th>AVL_TOTAL</th>
      <th>AVL_BASE</th>
      <th>FULL_SALES_28</th>
      <th>SALES_28</th>
      <th>FORECAST_LAST</th>
      <th>FORECAST1</th>
      <th>FORECAST2</th>
      <th>FECHA_ENTRADA</th>
      <th>DIAS_VENTA_DISPONIBLE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Bogotá</td>
      <td>286</td>
      <td>BOG ORO</td>
      <td>Despensa y productos secos</td>
      <td>Snacks y confitería</td>
      <td>Gomitas y caramelos</td>
      <td>435192</td>
      <td>7702993041277</td>
      <td>Gomas Gusanos Acidos - Trululu - 1 Und - Trulu...</td>
      <td>Super De Alimentos</td>
      <td>...</td>
      <td>23.0</td>
      <td>0.521739</td>
      <td>0.521739</td>
      <td>24.0</td>
      <td>24.0</td>
      <td>19.5</td>
      <td>18.0</td>
      <td>18.0</td>
      <td>2023-04-03 12:00:43</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Bogotá</td>
      <td>283</td>
      <td>BOG ORO</td>
      <td>Bebidas</td>
      <td>Cervezas y sidras</td>
      <td>Cervezas</td>
      <td>432487</td>
      <td>83741531460</td>
      <td>CERVEZA SIXPACK BOTEL - Grolsch - 1 ud. - Grol...</td>
      <td>Global Wine &amp; Spirits</td>
      <td>...</td>
      <td>23.0</td>
      <td>1.000000</td>
      <td>1.000000</td>
      <td>13.0</td>
      <td>13.0</td>
      <td>5.0</td>
      <td>3.4</td>
      <td>3.4</td>
      <td>2023-03-31 13:18:03</td>
      <td>45.0</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 52 columns</p>
</div>



# CREACIÓN COLUMNA DOH


```python
df_15["DOH"]=(df_15["INVENTARIO_TURBO"] / (df_15["FORECAST1"]/7))
df_15["DOH_PP"]=((df_15["INVENTARIO_TURBO"] + df_15["PEDIDO_PENDIENTE"])/ (df_15["FORECAST1"]/7))
```

# AJUSTE PROVEEDORES


```python
df_15["PROVEEDOR_FINAL"].fillna(df_15["SUPPLIER_NAME"],inplace=True)
```

# CREACIÓN DE ALERTAS

### Alerta no ingreso en mucho tiempo


```python
today = datetime.today()
df_15['ALERTA_INGRESO'] = np.where((df_15["INVENTARIO_TURBO"] == 0) 
                                   & ((today - df_15["FECHA_ENTRADA"]) > pd.Timedelta(days=15)), 1, 0)
```

### Alerta posible desabastesimiento


```python
df_15['OC_DAYS']=df_15['OC_DAYS'].astype(str)
```


```python
#lunes
def buscar_uno(celda):
    if '1' in celda:
        return 'lunes'
    else:
        return '0'
#martes
def buscar_dos(celda):
    if '2' in celda:
        return 'martes'
    else:
        return '0'
#miércoles
def buscar_tres(celda):
    if '3' in celda:
        return 'miércoles'
    else:
        return '0'
#jueves
def buscar_cuatro(celda):
    if '4' in celda:
        return 'jueves'
    else:
        return '0'
#viernes
def buscar_cinco(celda):
    if '5' in celda:
        return 'viernes'
    else:
        return '0'
#sábado
def buscar_seis(celda):
    if '6' in celda:
        return 'sábado'
    else:
        return '0'
#domingo
def buscar_siete(celda):
    if '7' in celda:
        return 'domingo'
    else:
        return '0'
       
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_UNO'] = df_15['OC_DAYS'].apply(buscar_uno)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_DOS'] = df_15['OC_DAYS'].apply(buscar_dos)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_TRES'] = df_15['OC_DAYS'].apply(buscar_tres)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_CUATRO'] = df_15['OC_DAYS'].apply(buscar_cuatro)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_CINCO'] = df_15['OC_DAYS'].apply(buscar_cinco)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_SEIS'] = df_15['OC_DAYS'].apply(buscar_seis)
# Aplicar la función a la columna 'OC_DAYS' con el método apply()
df_15['CONTIENE_SIETE'] = df_15['OC_DAYS'].apply(buscar_siete)
```


```python
# crear un diccionario para mapear los días de la semana a números
dias = {'lunes': 1, 'martes': 2, 'miércoles': 3, 'jueves': 4, 'viernes': 5, 'sábado': 6, 'domingo': 7}

# convertir la columna 'LEAD_TIME' a objetos timedelta
df_15['LEAD_TIME'] = pd.to_timedelta(df_15['LEAD_TIME'], unit='d')

# sumar la columna LEAD_TIME a la columna CONTIENE_UNO
df_15['NUEVA_FECHA_1'] = df_15.apply(lambda x: dias[x['CONTIENE_UNO']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_UNO'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_2'] = df_15.apply(lambda x: dias[x['CONTIENE_DOS']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_DOS'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_3'] = df_15.apply(lambda x: dias[x['CONTIENE_TRES']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_TRES'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_4'] = df_15.apply(lambda x: dias[x['CONTIENE_CUATRO']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_CUATRO'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_5'] = df_15.apply(lambda x: dias[x['CONTIENE_CINCO']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_CINCO'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_6'] = df_15.apply(lambda x: dias[x['CONTIENE_SEIS']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_SEIS'] != '0' else 0, axis=1) % 7
df_15['NUEVA_FECHA_7'] = df_15.apply(lambda x: dias[x['CONTIENE_SIETE']] + x['LEAD_TIME'].days % 7 if x['CONTIENE_SIETE'] != '0' else 0, axis=1) % 7
```


```python
# convertir la columna 'LEAD_TIME' a objetos timedelta
df_15['LEAD_TIME'] = pd.to_timedelta(df_15['LEAD_TIME'], unit='d')
# obtener la fecha actual
now = pd.Timestamp.now()
# sumar la columna LEAD_TIME a la fecha actual para obtener la fecha correspondiente
df_15['NUEVA_FECHA'] = now
# cambiar el formato de la fecha a 'yyyy-mm-dd'
df_15['NUEVA_FECHA'] = df_15['NUEVA_FECHA'].dt.strftime('%Y-%m-%d')
```


```python
# Convertir la columna 'NUEVA_FECHA' a formato datetime
df_15['NUEVA_FECHA'] = pd.to_datetime(df_15['NUEVA_FECHA'])

# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_6'
for index, row in df_15.iterrows():
#LUNES
    if row['NUEVA_FECHA_1'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_1'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_1'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_2'
for index, row in df_15.iterrows():
    #MARTES
    if row['NUEVA_FECHA_2'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_2'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_2'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_3'
for index, row in df_15.iterrows():
#MIÉRCOLES
    if row['NUEVA_FECHA_3'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_3'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_3'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_4'
for index, row in df_15.iterrows():
#JUEVES
    if row['NUEVA_FECHA_4'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_4'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_4'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_5'
for index, row in df_15.iterrows():
#VIERNES
    if row['NUEVA_FECHA_5'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_5'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_5'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_6'
for index, row in df_15.iterrows():
#SÁBADO
    if row['NUEVA_FECHA_6'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_6'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_6'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# Recorrer el DataFrame y actualizar la columna 'NUEVA_FECHA_7'
for index, row in df_15.iterrows():
#DOMINGO
    if row['NUEVA_FECHA_7'] == 1:
        lunes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=MO(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = lunes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 2:
        martes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TU(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = martes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 3:
        miercoles_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=WE(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = miercoles_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 4:
        jueves_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=TH(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = jueves_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 5:
        viernes_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=FR(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = viernes_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 6:
        sabado_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SA(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = sabado_de_la_semana.strftime('%Y-%m-%d')
    elif row['NUEVA_FECHA_7'] == 7:
        domingo_de_la_semana = row['NUEVA_FECHA'] + relativedelta(weekday=SU(0))
        df_15.at[index, 'NUEVA_FECHA_7'] = domingo_de_la_semana.strftime('%Y-%m-%d')
```


```python
# convert the date columns to datetime objects
date_cols = [col for col in df_15.columns if 'NUEVA_FECHA_' in col]
for col in date_cols:
    df_15[col] =pd.to_datetime(df_15[col])
# find the closest date to today
today = datetime.today()
df_15['SIGUIENTE_DESPACHO'] = df_15[date_cols].apply(lambda x: min(x, key=lambda y:abs(y-today)),axis=1)
```


```python
#Eliminar las columnas que ya no nos sirven
df_15.drop(columns=['CONTIENE_UNO','CONTIENE_DOS','CONTIENE_TRES','CONTIENE_CUATRO','CONTIENE_CINCO','CONTIENE_SEIS',
                    'CONTIENE_SIETE','NUEVA_FECHA_1','NUEVA_FECHA_2','NUEVA_FECHA_3','NUEVA_FECHA_4','NUEVA_FECHA_5',
                    'NUEVA_FECHA_6','NUEVA_FECHA_7','NUEVA_FECHA'],inplace=True)
```


```python
#Creación de la alerta de POSIBLE_DESABASTECIMIENTO
today_day = datetime.combine(datetime.today(), datetime.min.time())
df_15['DIAS_CUBRIR']=(df_15['SIGUIENTE_DESPACHO'] - today_day).dt.days

df_15['ALERTA_DESABASTECIMIENTO'] = df_15.apply(lambda row: 1 if row['DOH'] < row['DIAS_CUBRIR'] else 0, axis=1)
```


```python
df_15['DIAS_CUBRIR'] = np.where(df_15['DIAS_CUBRIR'] < 0, '0', df_15['DIAS_CUBRIR'])
```

### Alerta vida útil


```python
df_15['ALERTA_VIDA_UTIL'] = np.where((df_15["DOH_PP"] > df_15["DIAS_VENTA_DISPONIBLE"]), 1, 0)
```

### Alerta ¿En cuántas tiendas está en WL y en cuántas está agotado?


```python
#Create a new dataframe to filter WL and necessary columns
columns_to_include = ['WAREHOUSEID','STOREREFERENCEID','PRODUCT_EAN',
                      'WL_TYPE','NAME','INVENTARIO_TURBO']
conteo = df_15.loc[:, columns_to_include]
condition = conteo['WL_TYPE'].isin(['1 Ideal', '3 Sustitute'])
filtered_df = conteo.loc[condition, :]
```


```python
count_by_product = filtered_df.groupby('STOREREFERENCEID').size().reset_index(name='PRODUCT_COUNT')

count_inventario_turbo_zero = filtered_df.groupby('STOREREFERENCEID')['INVENTARIO_TURBO'].apply(lambda x: (x == 0).sum()).reset_index(name='INVENTARIO_TURBO_ZERO_COUNT')

final_df = pd.merge(count_by_product, count_inventario_turbo_zero, on='STOREREFERENCEID')

alerta_conteo = pd.merge(filtered_df, final_df, on='STOREREFERENCEID')

alerta_conteo['%_AGOTADOS'] = alerta_conteo['INVENTARIO_TURBO_ZERO_COUNT'] / alerta_conteo['PRODUCT_COUNT']
```

### Alerta ingreso PP 


```python
df_15["ALERTA_PP"] = df_15["DOH_PP"].apply(lambda x: 1 if x > 20 else 0)
```

### Hallar SWA por tienda, categoría 2, categoría 3, nacional, proveedor 


```python
def calculate_swa(df, groupby_col, sum_col, mul_col, unit_penetration, swa, oportunidad):
    # Group the DataFrame by the groupby column and calculate the sum of the sum column
    grouped_df = df.groupby(groupby_col).agg({sum_col: 'sum'})
    # Assign the name 'VENTAS' to the new column
    grouped_df.rename(columns={sum_col: 'VENTAS'}, inplace=True)
    # Merge the new column back into the original DataFrame
    df = pd.merge(left = df, right = grouped_df, how = 'left', on = groupby_col)
    # Calculate the new column by dividing the sum_col column by the 'VENTAS' column THIS IS UNIT_PENETRATION
    df[unit_penetration] = df[sum_col] / df['VENTAS']
    #Calculate SWA multiplying AVL_TOTAL and UNIT_PENETRATION_TIENDA
    df[swa] = df[mul_col] * df[unit_penetration]
    #Calculate SWA OPORTUNITY subtracting the SWA and UNIT_PENETRATION
    df[oportunidad] = df[unit_penetration] - df[swa]
    # Drop the 'VENTAS' column as it is no longer needed
    df.drop(columns=['VENTAS'], inplace=True)
    return df
```


```python
#SWA tienda
df_16 = calculate_swa(df_15, 'WAREHOUSEID', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_TIENDA', 'SWA_TIENDA', 
                      'OPORTUNIDAD_TIENDA')
df_16.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CITY</th>
      <th>WAREHOUSEID</th>
      <th>SCOPE_x</th>
      <th>CATEGORY_ONE</th>
      <th>CATEGORY_TWO</th>
      <th>CATEGORY_THREE</th>
      <th>STOREREFERENCEID</th>
      <th>PRODUCT_EAN</th>
      <th>NAME</th>
      <th>MAKER</th>
      <th>...</th>
      <th>DOH_PP</th>
      <th>ALERTA_INGRESO</th>
      <th>SIGUIENTE_DESPACHO</th>
      <th>DIAS_CUBRIR</th>
      <th>ALERTA_DESABASTECIMIENTO</th>
      <th>ALERTA_VIDA_UTIL</th>
      <th>ALERTA_PP</th>
      <th>UNIT_PENETRATION_TIENDA</th>
      <th>SWA_TIENDA</th>
      <th>OPORTUNIDAD_TIENDA</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Bogotá</td>
      <td>286</td>
      <td>BOG ORO</td>
      <td>Despensa y productos secos</td>
      <td>Snacks y confitería</td>
      <td>Gomitas y caramelos</td>
      <td>435192</td>
      <td>7702993041277</td>
      <td>Gomas Gusanos Acidos - Trululu - 1 Und - Trulu...</td>
      <td>Super De Alimentos</td>
      <td>...</td>
      <td>8.555556</td>
      <td>0</td>
      <td>2023-04-04</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.001077</td>
      <td>0.000562</td>
      <td>0.000515</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Bogotá</td>
      <td>283</td>
      <td>BOG ORO</td>
      <td>Bebidas</td>
      <td>Cervezas y sidras</td>
      <td>Cervezas</td>
      <td>432487</td>
      <td>83741531460</td>
      <td>CERVEZA SIXPACK BOTEL - Grolsch - 1 ud. - Grol...</td>
      <td>Global Wine &amp; Spirits</td>
      <td>...</td>
      <td>14.411765</td>
      <td>0</td>
      <td>2023-04-06</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.000411</td>
      <td>0.000411</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 63 columns</p>
</div>




```python
#SWA producto
df_16['COUNTRY'] = 'CO'
df_17 = calculate_swa(df_16, 'COUNTRY', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_PRODUCTO', 'SWA_PRODUCTO', 
                      'OPORTUNIDAD_PRODUCTO')
df_17.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CITY</th>
      <th>WAREHOUSEID</th>
      <th>SCOPE_x</th>
      <th>CATEGORY_ONE</th>
      <th>CATEGORY_TWO</th>
      <th>CATEGORY_THREE</th>
      <th>STOREREFERENCEID</th>
      <th>PRODUCT_EAN</th>
      <th>NAME</th>
      <th>MAKER</th>
      <th>...</th>
      <th>ALERTA_DESABASTECIMIENTO</th>
      <th>ALERTA_VIDA_UTIL</th>
      <th>ALERTA_PP</th>
      <th>UNIT_PENETRATION_TIENDA</th>
      <th>SWA_TIENDA</th>
      <th>OPORTUNIDAD_TIENDA</th>
      <th>COUNTRY</th>
      <th>UNIT_PENETRATION_PRODUCTO</th>
      <th>SWA_PRODUCTO</th>
      <th>OPORTUNIDAD_PRODUCTO</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Bogotá</td>
      <td>286</td>
      <td>BOG ORO</td>
      <td>Despensa y productos secos</td>
      <td>Snacks y confitería</td>
      <td>Gomitas y caramelos</td>
      <td>435192</td>
      <td>7702993041277</td>
      <td>Gomas Gusanos Acidos - Trululu - 1 Und - Trulu...</td>
      <td>Super De Alimentos</td>
      <td>...</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.001077</td>
      <td>0.000562</td>
      <td>0.000515</td>
      <td>CO</td>
      <td>0.000010</td>
      <td>0.000005</td>
      <td>0.000005</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Bogotá</td>
      <td>283</td>
      <td>BOG ORO</td>
      <td>Bebidas</td>
      <td>Cervezas y sidras</td>
      <td>Cervezas</td>
      <td>432487</td>
      <td>83741531460</td>
      <td>CERVEZA SIXPACK BOTEL - Grolsch - 1 ud. - Grol...</td>
      <td>Global Wine &amp; Spirits</td>
      <td>...</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.000411</td>
      <td>0.000411</td>
      <td>0.000000</td>
      <td>CO</td>
      <td>0.000006</td>
      <td>0.000006</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 67 columns</p>
</div>




```python
#SWA proveedor
df_18 = calculate_swa(df_17, 'PROVEEDOR_FINAL', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_PROVEEDOR', 'SWA_PROVEEDOR', 
                      'OPORTUNIDAD_PROVEEDOR')
```


```python
#SWA categoría 2
df_19 = calculate_swa(df_18, 'CATEGORY_TWO', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_CATEGORY2', 'SWA_CATEGORY2', 
                      'OPORTUNIDAD_CATEGORY2')
```


```python
#SWA categoría 3
df_20 = calculate_swa(df_19, 'CATEGORY_THREE', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_CATEGORY3', 'SWA_CATEGORY3', 
                      'OPORTUNIDAD_CATEGORY3')
```


```python
#SWA city
df_21 = calculate_swa(df_20, 'CITY', 'FULL_SALES_28', 'AVL_TOTAL', 'UNIT_PENETRATION_CITY', 'SWA_CITY', 
                      'OPORTUNIDAD_CITY')
```

### Creación de tablas de SWA para adjuntar en el correo


```python
resumen_swa_tienda = df_21.groupby("NOMBRE_TIENDA").agg({"SWA_TIENDA": "sum", 
                                     "OPORTUNIDAD_TIENDA": "sum", 
                                     "AVL_TOTAL": "mean"}).sort_values(by="OPORTUNIDAD_TIENDA", ascending=False) * 100
resumen_swa_tienda = resumen_swa_tienda.round(2)
```

# AJUSTES DE FORMATOS EXCEL


```python
df_21 = df_21.loc[:,['CITY','LOCATIONID','DEPENDENCIA','WAREHOUSEID','NOMBRE_TIENDA','CATEGORY_ONE','CATEGORY_TWO',
                    'CATEGORY_THREE','WL_TYPE','SCOPE_x','STOREREFERENCEID','PRODUCT_EAN','PLU_PADRE','PLU_HIJO','FACTOR',
                    'NAME','PARENTSTOREREFERENCEID','NUM_SUSTITUTOS','BUCKET','ESTADO_PLU','INVENTARIO_TURBO',
                    'INVENTARIO_EXITO','STOCK_PRESENTACION','UMD','PEDIDO_PENDIENTE','CEDI','NIT_PROVEEDOR_EXITO',
                    'PROVEEDOR_FINAL','SUPPLIER_NAME','AVL_TOTAL','AVL_BASE','SALES_28','SALES_AYER','FORECAST_LAST',
                    'FORECAST1','FORECAST2','DOI_GLOBAL','DOH','DOH_PP','OC_DAYS','LEAD_TIME','OTIF_DIRECTO','OTIF_CEDI',
                    'SIGUIENTE_DESPACHO','DIAS_CUBRIR','SWA_TIENDA','OPORTUNIDAD_TIENDA','SWA_PRODUCTO','OPORTUNIDAD_PRODUCTO',
                    'SWA_PROVEEDOR','OPORTUNIDAD_PROVEEDOR','SWA_CATEGORY2','OPORTUNIDAD_CATEGORY2',
                    'SWA_CATEGORY3','OPORTUNIDAD_CATEGORY3','SWA_CITY','OPORTUNIDAD_CITY','FECHA_ENTRADA',
                     'DIAS_VENTA_DISPONIBLE','ALERTA_INGRESO','ALERTA_DESABASTECIMIENTO','ALERTA_VIDA_UTIL','ALERTA_PP',
                     'KEEP_IN_WL']]
```


```python
fecha_file = datetime.today().strftime('%Y-%m-%d')
df_21.to_excel(f"Informe_SWA_{fecha_file}.xlsx", sheet_name="Resumen_SWA", index=False)
```


```python
df_existente = pd.read_excel(f"Informe_SWA_{fecha_file}.xlsx")
with pd.ExcelWriter(f"Informe_SWA_{fecha_file}.xlsx", engine='openpyxl', mode='a') as writer:
   alerta_conteo.to_excel(writer, sheet_name='Alerta_agotado_%', index=False)
```


```python
df_existente = pd.read_excel(f"Informe_SWA_{fecha_file}.xlsx")
with pd.ExcelWriter(f"Informe_SWA_{fecha_file}.xlsx", engine='openpyxl', mode='a') as writer:
   resumen_swa_tienda.to_excel(writer, sheet_name='SWA_tienda', index=True)
```

