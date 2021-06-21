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
from keras.models import Sequential
from keras.layers import Dense
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import urllib
import sqlalchemy


#DELIVERY DATA
conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD06',user='OI_METRICS',password='OI_METRICS')
cursor = conn.cursor()
Delivery_q = pd.read_sql_query('''
set transaction isolation level Read Uncommitted
SET NOCOUNT ON
Declare @FAC_OID OID
Set @FAC_OID=0x8E3074D7400A9854
--main table
if object_id('tempdb.dbo.#finale') is not null drop table #finale
if object_id('tempdb.dbo.#final') is not null drop table #final
--LOT MOVES + inprogress
if object_id('tempdb.dbo.#temp1') is not null drop table #temp1
if object_id('tempdb.dbo.#temp2') is not null drop table #temp2
if object_id('tempdb.dbo.#temp3') is not null drop table #temp3
if object_id('tempdb.dbo.#temp4') is not null drop table #temp4
if object_id('tempdb.dbo.#TestWafers') is not null drop table #TestWafers
if object_id('tempdb.dbo.#Stockerlist') is not null drop table #Stockerlist
if object_id('tempdb.dbo.#delivery') is not null drop table #delivery
if object_id('tempdb.dbo.#deliver2') is not null drop table #deliver2
if object_id('tempdb.dbo.#TWMSetupInfo') is not null drop table #TWMSetupInfo
if object_id('tempdb.dbo.#view') is not null drop table #view
if object_id('tempdb.dbo.#stuff') is not null drop table #stuff
if object_id('tempdb.dbo.#bws_wafer_cnt') is not null drop table #bws_wafer_cnt
if object_id('tempdb.dbo.#bws_wafer_capacity') is not null drop table #bws_wafer_capacity
if object_id('tempdb.dbo.#util') is not null drop table #util


CREATE TABLE #TWMSetupInfo (
TWType		VARCHAR(50) NOT NULL
,Stocker		VARCHAR(255) NOT NULL
,tot_min_qty	INT NULL
,tot_max_qty	INT NULL
)
INSERT INTO
#TWMSetupInfo
SELECT
tw_type
,storage_location
,SUM(isnull(min_qty,0)) AS min_qty
,SUM(isnull(max_qty,0)) AS max_qty
FROM
fab_lot_extraction..TWM_start_setup
GROUP BY
tw_type
,storage_location
ORDER BY
tw_type DESC
,storage_location DESC



CREATE TABLE #bws_wafer_cnt
(
equip_id      varchar(20)  NOT NULL,
wafer_cnt     real         NOT NULL,
)
CREATE UNIQUE INDEX IDX_bws_wafer_cnt_EQUIP_ID ON #bws_wafer_cnt( equip_id )
CREATE TABLE #bws_wafer_capacity
(
equip_id      varchar(20)  NOT NULL,
capacity      real         NULL
)
CREATE UNIQUE INDEX IDX_bws_wafer_capacity_EQUIP_ID ON #bws_wafer_capacity( equip_id )
INSERT INTO #bws_wafer_cnt (equip_id, wafer_cnt)
SELECT [equip_id]
,COUNT(*)   as wafer_cnt
FROM [fab_lot_extraction].[dbo].[stocker_map_wafer]
WHERE [equip_id] IN
(SELECT equip_id
FROM [equip_tracking_DSS].[dbo].[equipment]
WHERE equip_type_id  = 'WAFER STOCKER'
AND equip_status   = 'ACTIVE')
GROUP BY [equip_id]
ORDER BY [equip_id]
INSERT INTO #bws_wafer_capacity (equip_id, capacity)
SELECT  eq.equip_id                equip_id
,eq_item.value              equip_item_value
FROM  equip_tracking_DSS.dbo.equipment              eq      INNER JOIN
equip_tracking_DSS.dbo.equip_item_level_def   eq_item
ON  eq.equip_OID                                = eq_item.level_assoc_OID
WHERE RTRIM(eq.equip_type_id) = 'WAFER STOCKER'
AND eq_item.equip_item_no   = 1024
ORDER BY equip_id


SELECT cnt.equip_id
,wafer_cnt
,capacity
,ROUND(wafer_cnt/capacity * 100, 2) as util
into #util
FROM  #bws_wafer_cnt      cnt       LEFT OUTER JOIN
#bws_wafer_capacity cap
ON  cnt.equip_id      = cap.equip_id
ORDER BY equip_id


------------------DELIVERY
select 
distinct
 wa.wafer_attr_value 
into #Stockerlist
from 
traveler..corr_item CITEM
INNER JOIN fab_lot_extraction..wafer_attr wa
ON CITEM.corr_item_OID = wa.corr_item_OID
WHERE
CITEM.corr_item_type = 'WAFER ATTRIBUTE'
AND RTRIM(CITEM.corr_item_desc) IN ('WLA STORAGE LOCATION')
AND wa.wafer_attr_value IS NOT NULL



SELECT  q.* ,
--DATEDIFF(second,request_time,arrival_time) as Time,
case when q.source_device in (select distinct S.wafer_attr_value from #Stockerlist S) 
then  q.source_device else '' end as [source],

case when substring(q.destination_device,1,7) in  (substring(f.chamber,1,7)  COLLATE SQL_Latin1_General_CP1_CI_AS)
then  q.destination_device else '' end as [destination],

case when 
substring(q.source_device ,7,2) like '[0-9][A-Z]' then 'F10A' when
substring(q.source_device ,7,2) like '[A-Z][A-Z]' then 'F10X' when
substring(q.source_device ,7,2) like '[0-9][0-9]' then 'F10N' when
substring(q.source_device ,7,2) like '[A-Z][0-9]' then 'F10N' end as [source location],
case when 
substring(q.destination_location,7,2) like '[0-9][A-Z]' then 'F10A' when
substring(q.destination_location,7,2) like '[A-Z][A-Z]' then 'F10X' when
substring(q.destination_location,7,2) like '[0-9][0-9]' then 'F10N' when
substring(q.destination_location,7,2) like '[A-Z][0-9]' then 'F10N' end as [destination location],
f.chamber,
f.site as chamber_site,
--d.[destination location],
f.qual_definition,
f.TWType
into #delivery
FROM [AMHS_Software_Reports].[dbo].[amhs_move_detail] q WITH(NOLOCK) 
inner join [SIC_METRICS].[SIC_METRICS].[ML_DeliveryTime] f WITH(NOLOCK) 
on 
f.lot_id = q.lot_id  COLLATE SQL_Latin1_General_CP1_CI_AS
where
q.part_type = 'TW'
and q.request_time > (select cast(cast(ww.mfg_ww_begin_datetime as date) as datetime)  from [reference].[dbo].[mfg_year_month_ww] ww where GETDATE()-14  between ww.mfg_ww_begin_datetime and ww.mfg_ww_end_datetime)
and 
(
q.source_device in (select distinct S.wafer_attr_value from #Stockerlist S) 
or
substring(q.destination_device,1,7) in  (substring(f.chamber,1,7)  COLLATE SQL_Latin1_General_CP1_CI_AS)
)


---------------------




select 
d.amhs_system,
d.system,
d.lot_id,
d.chamber,
d.qual_definition,
d.TWType,
d.[source location],
d.chamber_site,
max(d.[source]) as [source],
max(d.[destination]) as [destination],
min(request_time) as start_t,
max(arrival_time) as end_t
into #deliver2
from #delivery d
group by 
d.amhs_system,
d.system,
d.lot_id,
d.chamber,
d.qual_definition,
d.TWType,
d.[source location],
d.chamber_site



select 
d.amhs_system,
d.system,
d.lot_id,
d.source,
d.destination,
d.chamber,
DATEDIFF(second,d.start_t,d.end_t) as Time,
d.[source location] as source_site,
d.chamber_site,
d.qual_definition,
d.TWType,
start_t,
end_t
into #view
from
#deliver2 d

--where


--------------------------------------------------------------------------------------------------------------------ML --PARAMETER
--d.TWType  in  ('MT_6KDCOXID','MT_BAREHARP','MT_BRBEOXID')  
-------------------------------------------------------------------------------------------------------------------------------



--SELECT  * ,
--DATEDIFF(second,request_time,arrival_time) as Time,
--case when 
--substring(q.source_device ,7,2) like '[0-9][A-Z]' then 'F10A' when
--substring(q.source_device ,7,2) like '[A-Z][A-Z]' then 'F10X' when
--substring(q.source_device ,7,2) like '[0-9][0-9]' then 'F10N' when
--substring(q.source_device ,7,2) like '[A-Z][0-9]' then 'F10N' end as [source location],
--case when 
--substring(q.destination_location,7,2) like '[0-9][A-Z]' then 'F10A' when
--substring(q.destination_location,7,2) like '[A-Z][A-Z]' then 'F10X' when
--substring(q.destination_location,7,2) like '[0-9][0-9]' then 'F10N' when
--substring(q.destination_location,7,2) like '[A-Z][0-9]' then 'F10N' end as [destination location]
--FROM [AMHS_Software_Reports].[dbo].[amhs_move_detail] q WITH(NOLOCK) 
--inner join #final f on 
--f.lot_id = q.lot_id
--and f.Date =  cast (q.request_time as date)
--where
--q.part_type = 'TW'
--and q.request_time > (select cast(cast(ww.mfg_ww_begin_datetime as date) as datetime)  from [reference].[dbo].[mfg_year_month_ww] ww where GETDATE()-14  between ww.mfg_ww_begin_datetime and ww.mfg_ww_end_datetime)
--and move_from_type = 'FROMSTOCKER'




select distinct
v.TWType,
v.source,
source_site,
chamber_site,
TWI.tot_min_qty as [Min],
TWI.tot_max_qty as [Max],
util.capacity,
min(v.Time) AS Record_time
into #stuff 
from #view v
left join #TWMSetupInfo TWI 
on 
TWI.Stocker = v.source  COLLATE SQL_Latin1_General_CP1_CI_AS
and TWI.TWType = v.TWType  COLLATE SQL_Latin1_General_CP1_CI_AS
left join #util util
on
util.equip_id = v.source COLLATE SQL_Latin1_General_CP1_CI_AS
where
v.source_site = v.chamber_site COLLATE SQL_Latin1_General_CP1_CI_AS
and TWI.tot_max_qty is not null
and TWI.tot_min_qty is not null
group by
v.TWType,
v.source,
source_site,
chamber_site,
TWI.tot_min_qty,
TWI.tot_max_qty,
util.capacity





select 
s.TWType,
s.source_site,
s.chamber_site,
STUFF((select ', ' +  cast(s1.source as varchar)
		from #stuff s1
		where 
		s1.TWType = s.TWType and
		s1.chamber_site = s.chamber_site and
		s1.source_site = s.source_site and
		s1.chamber_site = s.chamber_site
		order by s1.Record_time asc
		FOR XML PATH('')), 1, 1, '') [Source_devices],
STUFF((select ', ' +  cast(s1.Record_time as varchar)
		from #stuff s1
		where 
		s1.TWType = s.TWType and
		s1.chamber_site = s.chamber_site and
		s1.source_site = s.source_site and
		s1.chamber_site = s.chamber_site
		order by s1.Record_time asc
		FOR XML PATH('')), 1, 1, '') [Time_(seconds)],
STUFF((select ', ' +  cast(s1.[Min] as varchar)
		from #stuff s1
		where 
		s1.TWType = s.TWType and
		s1.chamber_site = s.chamber_site and
		s1.source_site = s.source_site and
		s1.chamber_site = s.chamber_site
		order by s1.Record_time asc
		FOR XML PATH('')), 1, 1, '') [Min],
STUFF((select ', ' +  cast(s1.[Max] as varchar)
		from #stuff s1
		where 
		s1.TWType = s.TWType and
		s1.chamber_site = s.chamber_site and
		s1.source_site = s.source_site and
		s1.chamber_site = s.chamber_site
		order by s1.Record_time asc
		FOR XML PATH('')), 1, 1, '') [Max],
STUFF((select ', ' +  cast(s1.capacity as varchar)
		from #stuff s1
		where 
		s1.TWType = s.TWType and
		s1.chamber_site = s.chamber_site and
		s1.source_site = s.source_site and
		s1.chamber_site = s.chamber_site
		order by s1.Record_time asc
		FOR XML PATH('')), 1, 1, '') [capacity]
from #stuff s
group by
s.TWType, s.source_site, s.chamber_site

--select * from #stuff

'''
,conn) #, params=TWTYPE)
Delivery_d=pd.DataFrame(Delivery_q)

QUALHIST = Delivery_d
#QUALHIST['Date'] = pd.to_datetime(QUALHIST['Date'])


DATATABLEFROMPYTHON = QUALHIST
print(DATATABLEFROMPYTHON)


def main(df,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema):
    connection_string= "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;" % (SQL_server, Database, SQL_user, SQL_pwd)
    connection_string = urllib.parse.quote_plus(connection_string)
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name_in_SQL,engine,schema =schema, if_exists='replace', chunksize=200, index = False) #replace or append
    return df



table_name_in_SQL = 'Delivery_info' #replace SQLTABLETOUPLOAD
main(DATATABLEFROMPYTHON,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema) #replace DATATABLEFROMPYTHON











