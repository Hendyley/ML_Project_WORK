# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 09:32:05 2020

@author: hendya
"""

#upload data to SQL database
SQL_server = 'FSMSSPROD170,54059' #replace port number, check using below code
Database = 'MFG_METRICS'
SQL_user ='f10prod'
SQL_pwd = 'Micron123'
schema = 'MFG_METRICS'




import numpy as np
import pandas as pd
import pyodbc 
from numpy import loadtxt
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import urllib
import sqlalchemy


#DELIVERY DATA
conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD06',user='f10prod',password='2020BestYear')
cursor = conn.cursor()
BWS_q = pd.read_sql_query('''
set transaction isolation level Read Uncommitted
SET NOCOUNT ON

if object_id('tempdb.dbo.#bws_wafer_capacity') is not null drop table #bws_wafer_capacity

CREATE TABLE #bws_wafer_capacity
(
equip_id      varchar(20)  NOT NULL,
capacity      real         NULL
)

INSERT INTO #bws_wafer_capacity (equip_id, capacity)
SELECT  eq.equip_id                equip_id
,eq_item.value              equip_item_value
FROM  equip_tracking_DSS.dbo.equipment              eq      INNER JOIN
equip_tracking_DSS.dbo.equip_item_level_def   eq_item
ON  eq.equip_OID                                = eq_item.level_assoc_OID
WHERE RTRIM(eq.equip_type_id) = 'WAFER STOCKER'
AND eq_item.equip_item_no   = 1024
ORDER BY equip_id



SELECT distinct
tw_type,
storage_location,
cap.capacity,
case when 
substring(storage_location,7,2) like '[0-9][A-Z]' then 'F10A' when
substring(storage_location,7,2) like '[A-Z][A-Z]' then 'F10X' when
substring(storage_location,7,2) like '[0-9][0-9]' then 'F10N' when
substring(storage_location,7,2) like '[A-Z][0-9]' then 'F10N' end as [location],
SUM(isnull(min_qty,0)) AS min_qty
,SUM(isnull(max_qty,0)) AS max_qty
FROM
fab_lot_extraction..TWM_start_setup s
left join #bws_wafer_capacity cap on cap.equip_id = s.storage_location
where
storage_location <> 'CARRIER'-- and cap.capacity is not null
GROUP BY
tw_type
,storage_location
,cap.capacity
ORDER BY
tw_type DESC
,storage_location DESC
'''
,conn) #, params=TWTYPE)
BWS=pd.DataFrame(BWS_q)


DATATABLEFROMPYTHON = BWS
#print(DATATABLEFROMPYTHON)


def main(df,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema):
    connection_string= "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;" % (SQL_server, Database, SQL_user, SQL_pwd)
    connection_string = urllib.parse.quote_plus(connection_string)
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name_in_SQL,engine,schema =schema, if_exists='replace', chunksize=200, index = False) #replace or append
    return df



table_name_in_SQL = 'BWS_TWTYPE_ALLOCATION' #replace SQLTABLETOUPLOAD
main(DATATABLEFROMPYTHON,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema) #replace DATATABLEFROMPYTHON











