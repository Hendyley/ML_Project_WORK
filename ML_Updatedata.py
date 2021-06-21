# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 14:31:57 2020

@author: hendya
"""
import win32com.client
import numpy as np
import pandas as pd
import pyodbc 
from numpy import loadtxt
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import urllib
import sqlalchemy
import datetime


################################################################################       Qual run history
print("\n" + "Start process " + str(datetime.datetime.now()))
print("Updating Qual_run.xlsx\n")

xlapp = win32com.client.DispatchEx("Excel.Application")
wb = xlapp.Workbooks.Open(r"C:\Users\heyudao\Desktop\ML\Qual_RUN.xlsx")
wb.RefreshAll()
xlapp.CalculateUntilAsyncQueriesDone()
wb.Save()
xlapp.Quit()


SQL_server = 'FSMSSPROD170,54059' #replace port number, check using below code
Database = 'MFG_METRICS'
SQL_user ='f10prod'
SQL_pwd = 'Micron123'
schema = 'MFG_METRICS'

print("\n" + "start process " + str(datetime.datetime.now()))
print("Updating Qual_run_History prod170\n")

QUALHIST = pd.read_excel(r'C:\Users\heyudao\Desktop\ML\Qual_RUN.xlsx').dropna()
QUALHIST['Date'] = pd.to_datetime(QUALHIST['Date'])


DATATABLEFROMPYTHON = QUALHIST
print(DATATABLEFROMPYTHON)


def main(df,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema):
    connection_string= "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;" % (SQL_server, Database, SQL_user, SQL_pwd)
    connection_string = urllib.parse.quote_plus(connection_string)
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name_in_SQL,engine,schema =schema, if_exists='replace', chunksize=200, index = False) #replace or append
    return df



table_name_in_SQL = 'Qual_Run_History' #replace SQLTABLETOUPLOAD
main(DATATABLEFROMPYTHON,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema) #replace DATATABLEFROMPYTHON



################################################################################       TWTYPE Count

print("\n" + "start process " + str(datetime.datetime.now()))
print("Updating TWType Count prod170\n")

conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD170',user='f10prod',password='Micron123')
cursor = conn.cursor()
cursor.execute('''
set transaction isolation level Read Uncommitted
SET NOCOUNT ON

declare @StartDate date = (select cast(cast(ww.mfg_ww_begin_datetime as date) as datetime)  from [reference].[dbo].[mfg_year_month_ww] ww where GETDATE()-180  between ww.mfg_ww_begin_datetime and ww.mfg_ww_end_datetime)
declare @EndDate date = getdate();

if object_id('tempdb.dbo.#t1') is not null drop table #t1
if object_id('tempdb.dbo.#t2') is not null drop table #t2
if object_id('tempdb.dbo.#t3') is not null drop table #t3
if object_id('tempdb.dbo.#t4') is not null drop table #t4
if object_id('tempdb.dbo.#t5') is not null drop table #t5
if object_id('tempdb.dbo.#date') is not null drop table  #date
if object_id('tempdb.dbo.#date2') is not null drop table  #date2
if object_id('tempdb.dbo.#final') is not null drop table  #final 

;WITH cte AS (
    SELECT @StartDate AS Date
    UNION ALL
    SELECT DATEADD(day,1,Date) as Date
    FROM cte
    WHERE DATEADD(day,1,Date) <=  @EndDate
)
SELECT  Date
into #date
FROM cte
OPTION (MAXRECURSION 0)

SELECT *  
into #t1
FROM [MFG_METRICS].[MFG_METRICS].[ALL_TWType_Run_By_Qual] q
where
q.[QUAL DEFINITION] in (SELECT distinct t.qual_definition FROM [MFG_METRICS].[MFG_METRICS].[Qual_Run_History] t)
and q.Count <> 0

select 
[MFG AREA],
[QUAL DEFINITION],
Count,
u.[TWType],
u.[Totalrun]
into #t2
from #t1 t1
Unpivot
(
[Totalrun]
for [TWType] in
(
[AM_CY-GENBILD]
,[AM_O2PBAREBARE]
,[AM_PM1CCUBRBARE]
,[AM_PM1NBAREBARE]
,[AM_ST-CU-FLM1]
,[AM-PXOBAREBARE]
,[AS_ER-PSGBILD]
,[AS_ERRESTBILD]
,[CC-PMNODPBILD]
,[CM_BIBPSGBILD]
,[CM_BIBSONBILD]
,[CM_BI-CU-BILD]
,[CM_BI-CU-CBLD]
,[CM_BINBUFBILD]
,[CM_BIRSSTBILD]
,[CM_BI-SOPBILD]
,[CM_BITUNGBILD]
,[CM_BITUNGBILD1]
,[CM_CYNOCUBILD]
,[CM_CYNOCUBILD1]
,[CM_CYNOCUBILD2]
,[CM_GWBPSGNOV2]
,[CM_GWBPSGNOVA]
,[CM_GWNANONSTB]
,[CM_GWNANONSTD]
,[CM_GWNANONSTH]
,[CM_GWNANONSTS]
,[CM_GWNANOQUA2]
,[CM_GWNANOQUAL]
,[CM_GW-SONNOVA]
,[CM_GW-SOPNOVA]
,[CM_PM1CTEOSCUCM]
,[CM_PM1N5KTEOSOX]
,[CM_PM1NHARPOXID]
,[CM_PM1NNITRRCLM]
,[CM_PM1NNITRRSST]
,[CM_PM1NTEOSOXID]
,[CM_RRTUNGRATE]
,[CM_ST-CU-BILD]
,[CM_VPNOCUBILD]
,[CV_BARECARB]
,[CV_BAREDARC]
,[CV_GWTVETFLM1]
,[CV_GWTVETFLM6]
,[CV_GWTVETFLM7]
,[CV_MT1N1KOXATIN]
,[CV_MT1N1KOXOPLY]
,[CV_MT1NBAREDARC]
,[CV_MT1NBRHDSPHA]
,[CV_NM1N6KOXVTCH]
,[CV_NM1NBRHDSPHA]
,[CV_PM1CCUBRCUOX]
,[CV_PM1N1KOX-TIN]
,[CV_PM1NBARE14CA]
,[CV_PM1NBAREBPSG]
,[CV_PM1NBARECARB]
,[CV_PM1NBAREHARP]
,[CV_PM1NBARENIT1]
,[CV_PM1NBARENITR]
,[CV_PM1NBAREONON]
,[CV_PM1NBAREOXID]
,[CV_PM1NBARESPHA]
,[CV_PM1NBARETHDC]
,[CV_PM1NBRBEFLM1]
,[CV_PM1NBRNDSPHA]
,[CV_PM1NSTINTUNG]
,[CV_SWHARPFLM2]
,[CV_SWSIOXBILD]
,[CV_SWTEOSBILD]
,[DE_CN82LVCBLD1]
,[DE_CN82LVCBLD2]
,[DE_CNCRSTBILD]
,[DE_CN-CU-80LV]
,[DE_CNNOCUBILD]
,[DE_CNNOCUFLM4]
,[DE_CNNOCUFLM5]
,[DE_CNNOCUFLM6]
,[DE_CNNOCUFLM8]
,[DE_CNNOCUFLM9]
,[DE_CN-RSTBILD]
,[DE_CN-RSTFLM1]
,[DE_CN-RSTFLM2]
,[DE_CNRSTFLM6]
,[DE_CN-RSTRG01]
,[DE_CUBRCUBR]
,[DE_CUOXSIOX]
,[DE_CURSCURS]
,[DE_CYBAREFLM1]
,[DE_CYBAREFLM2]
,[DE_CYBAREFLM5]
,[DE_GWBARESATEMP]
,[DE_GWBARESFLM1]
,[DE_MT1CNITRCUNT]
,[DE_MT1NBAREOXID]
,[DE_NM1NBAREVRK1]
,[DE_RSSTCOND]
,[DE_RSSTRSST]
,[DE_SW2NNOCUFLMG]
,[DE_SWNITRFILM1]
,[DE_SWNOCUFLMA]
,[DE_SWNOCUFLMB]
,[DE_SWNOCUFLMC]
,[DE_SWNOCUFLMD]
,[DE_SWNOCUFLME]
,[DE_SWRSTFLM3]
,[DE_SWRSTFLM4]
,[DE_SWRSTFLM5]
,[DE_SW-RSTRGSC]
,[DF_CN-RPO-RTO]
,[DF_CY-PIOFLM1]
,[DF_CY-PIOFLM2]
,[DF_CY-PISOFLM]
,[DF_FL2NBNITFLM1]
,[DF_FLCUANBILD]
,[DF_FLTUNGBILD]
,[DF_FLTUNGTHCK]
,[DF_GW-EMISCAL]
,[DF_MT1N1KOXFOGP]
,[DF_MT1NBARWOXID]
,[DF_NM1NNITRALOX]
,[DF_PM1CCUBR-DIF]
,[DF_PM1N1KOXPOLY]
,[DF_PM1NBAREALOX]
,[DF_PM1NBAREEKKI]
,[DF_PM1NBAREFOGP]
,[DF_PM1NBAREMTLS]
,[DF_PM1NBARENITR]
,[DF_PM1NBAREOXID]
,[DF_PM1NBARE-RTP]
,[DF_PM1NBARESCOX]
,[DF_PM1NBARWANNL]
,[DF_PM1NBRGAISSG]
,[DF_PM1NBRTHALOX]
,[DF_PM-CU-BILD]
,[DF_PMNITRBILD]
,[DF_WU-PIOFLM1]
,[DUMMY]
,[GE_BDOXIDBILD]
,[GE_PMOXIDBILD]
,[GOLDEN WAFER]
,[IM_BI-BF3FLM1]
,[IM_BI-BHFLM2]
,[IM_BIPLADFLM1]
,[IM_BIPLADFLM3]
,[IM_CN-BH-FLM2]
,[IM_NM1NWUPLAD01]
,[IM_NM1NWUPLAD02]
,[IM_PM1NBRS1NTOX]
,[IM_SW-NF3FLM2]
,[IM_SW-NF3FLM3]
,[MT_16KRRSST]
,[MT_1KCXMTLS]
,[MT_1KDCOXID]
,[MT_1KOXBPLY]
,[MT_1KOXCGOX]
,[MT_1KOXDARC]
,[MT_1KOXDGST]
,[MT_1KOXMTLS]
,[MT_1KOXMTOX]
,[MT_1KOXOPPLY]
,[MT_1KOXOPSTK]
,[MT_1KOXPLY1]
,[MT_1KOXPOLY]
,[MT_1KOXRCLM]
,[MT_1KOXTHDC]
,[MT_1KOX-TIN]
,[MT_1KOXWSIX]
,[MT_1KPYPOLY]
,[MT_1KPYPOLY1]
,[MT_33K-RSST]
,[MT_33KRSVTCH]
,[MT_6KDCBEVL]
,[MT_6KDCCVOX]
,[MT_6KDCOXID]
,[MT_6KDCOXID2]
,[MT_6KOXHKOX]
,[MT_BARE14CA]
,[MT_BAREALOX]
,[MT_BAREBPSG]
,[MT_BARECARB]
,[MT_BAREDARC]
,[MT_BAREHARP]
,[MT_BAREHDCA]
,[MT_BAREHKOX]
,[MT_BAREIMOX]
,[MT_BARELPRO]
,[MT_BARENITR]
,[MT_BARENTOX]
,[MT_BAREONON]
,[MT_BAREONTNIT]
,[MT_BAREOO3L]
,[MT_BAREOPHF]
,[MT_BAREOPSTK]
,[MT_BAREOPSTK2]
,[MT_BAREOPTOX]
,[MT_BAREOXID]
,[MT_BAREOXNT]
,[MT_BARERSST]
,[MT_BARESAPHA]
,[MT_BARE-SC1]
,[MT_BARE-SOD]
,[MT_BRBEFLM1]
,[MT_BRBEHTOX]
,[MT_BRBEOXID]
,[MT_BRBROXID]
,[MT_BRCVBPSG]
,[MT_BRCVDARC]
,[MT_BRCVNIT1]
,[MT_BRCVNIT2]
,[MT_BRCVNIT3]
,[MT_BRCVNITR]
,[MT_BRDE-OXID]
,[MT_BRDFMTLS]
,[MT_BRHTOPHF]
,[MT_CARBFILM]
,[MT_CARBSAPH]
,[MT_CUBR7KAL]
,[MT_CUBRCUOX]
,[MT_CUOXCUCU]
,[MT_CUOXSIOX]
,[MT_CUPY-CURE]
,[MT_CURS-CURE]
,[MT_CURSRCLM]
,[MT_CUSDBURN]
,[MT_CUSDCUCU]
,[MT_CUSIOXBEVL]
,[MT_CVPYPOLY]
,[MT_GWBAREVIBR]
,[MT_GW-CDSBCKQ]
,[MT_GW-CDSFLM2]
,[MT_GW-EPCDSQUAL]
,[MT_GW-OCDQUAL]
,[MT_GW-REGADPM]
,[MT_GW-REGAGLDN]
,[MT_GW-REGGLDN]
,[MT_GW-REGQDBU]
,[MT_GW-REGQDPM]
,[MT_ISSG-MTL]
,[MT_MTLSDOWN]
,[MT_NITRFLM1]
,[MT_NITRFLM2]
,[MT_NITRMTNITR]
,[MT_NITRNITR]
,[MT_NTOXOXID]
,[MT_OXIDBPLY]
,[MT_OXIDCGOX]
,[MT_OXIDDPN]
,[MT_OXIDPOLY]
,[MT_OXIDRTN]
,[MT_OXPYPOLY]
,[MT_POLY-CO]
,[MT_POLYPOLY]
,[MT_RRTUNGRATE]
,[MT_RSSTFLM2]
,[MT_RSSTHKRS]
,[MT_RSSTRSST]
,[MT_SAPHOMDS]
,[MT_SIOXVTCH]
,[MT_STINTUNG]
,[MT_TEMPPOLY]
,[MT_TEOSOXID]
,[MT_TEOSTITN]
,[MT_THTEO-CO]
,[MT_THTEO-GST]
,[MT_THTEOXID]
,[MT_TIN-BEVL]
,[MT_TIN-TUNG]
,[MT_TKDCOXID]
,[MT_TUNGFLM1]
,[MT_TUNGRCLM]
,[NM_1KOXOPPLY]
,[NM_6KDCALOX]
,[NM_6KDCOXID]
,[NM_6KDCSEON]
,[NM_6KOX-BKSD]
,[NM_6KOXOXID]
,[NM_BAREBARE]
,[NM_BAREFLM1]
,[NM_BAREFLM3]
,[NM_BARENITR]
,[NM_BAREOMNS]
,[NM_BAREONON]
,[NM_BAREOPSTK]
,[NM_BARERCLM]
,[NM_BARESAPHA]
,[NM_BARE-SC1]
,[NM_BARE-VPD]
,[NM_BPSGOXID]
,[NM_BPSG-SC1]
,[NM_BRBEFLM1]
,[NM_BRBERCLM]
,[NM_BRCUBEVL]
,[NM_BRCVNITR]
,[NM_BRCYISSG]
,[NM_BRDFALOX]
,[NM_CARBCOND]
,[NM_CARBRCLM]
,[NM_CARBSAPH]
,[NM_CARBSEON]
,[NM_CNNOCUBILD]
,[NM_CN-RSTFLM1]
,[NM_CN-RSTFLM2]
,[NM_CUOXCUOX]
,[NM_CUOXRCLM]
,[NM_CURSFLM3]
,[NM_CUTEOSFLM1]
,[NM_CUTEOSTIN]
,[NM_CUTUNGRCLM]
,[NM_CYBAREFILM]
,[NM_CYBAREFLMA]
,[NM_CYBAREFLMT]
,[NM_CYBAREFLMX]
,[NM_CYBAREFLMY]
,[NM_CYBAREFLMZ]
,[NM_CYBEVLCWAC]
,[NM_DARCRCLM]
,[NM_HARPRCLM]
,[NM_MTLSRCLM]
,[NM_NITRNITR]
,[NM_RSSTFLM1]
,[NM_SOD-RCLM]
,[NM_SWNOCUFLMA]
,[NM_TEOSOXID]
,[NM_TIN-RCLM]
,[NM_TUNGRCLM]
,[PC_ENGRRCLM]
,[PH_CYTRCKFLM1]
,[PH_GW-CDSQUAL]
,[PH_GW-PROVBACKUP]
,[PH_GW-PROVOPAL]
,[PH_GWREG1QUAL]
,[PH_GWREG2QUAL]
,[PH_GWREG3QUAL]
,[PH_GWREG4QUAL]
,[PH_GWREG5QUAL]
,[PH_GWSCANBF01]
,[PH_GWSCANLEBF]
,[PH_GWSCANLHBF]
,[PH_GWSCANRHBF]
,[PH_PM1NBARE-MLR]
,[PH_PM1NBARE-OPT]
,[PH_PM1NBAREPIMD]
,[PH_PMCRSTBILD]
,[PM_16K-RSST]
,[PM_1KDF-VPD]
,[PM_1KOXBEVL]
,[PM_1KOXBPLY]
,[PM_1KOXCGOX]
,[PM_1KOXDARC]
,[PM_1KOXLPOX]
,[PM_1KOXOPPLY]
,[PM_1KOXOXID]
,[PM_1KOXPOLY]
,[PM_1KOXRCLM]
,[PM_1KOX-TIN]
,[PM_1KOXVIGU]
,[PM_1KOX-VPD]
,[PM_1KOXVTCH]
,[PM_1KOXWSIX]
,[PM_2KCADARC]
,[PM_95OX-VPD]
,[PM_ALOXRCLM]
,[PM_AMHSBARE]
,[PM_AMHSCUBARE]
,[PM_ANL1OXID]
,[PM_ANL2OXID]
,[PM_ANNLNTOX]
,[PM_BARE14CA]
,[PM_BARE15CA]
,[PM_BAREALOX]
,[PM_BAREBARE]
,[PM_BAREBEVL]
,[PM_BAREBHOX]
,[PM_BAREBKSD]
,[PM_BARE-BOE]
,[PM_BAREBPSG]
,[PM_BARECARB]
,[PM_BARE-CO]
,[PM_BAREDARC]
,[PM_BAREDFST]
,[PM_BAREEKKI]
,[PM_BARE-GST]
,[PM_BAREHARP]
,[PM_BAREHDCA]
,[PM_BAREHKBR]
,[PM_BAREHKOX]
,[PM_BARELPRO]
,[PM_BARE-MLR]
,[PM_BAREMSC1]
,[PM_BAREMTLS]
,[PM_BAREMTSC1]
,[PM_BARE-MVQ]
,[PM_BARENITH]
,[PM_BARENITR]
,[PM_BARENMTL]
,[PM_BARENPHF]
,[PM_BAREONON]
,[PM_BAREOPHF]
,[PM_BAREOPSTK]
,[PM_BAREOPSTK2]
,[PM_BAREOPSTK3]
,[PM_BAREOPSTK4]
,[PM_BARE-OPT]
,[PM_BARE-OPT1]
,[PM_BAREOPTOX]
,[PM_BAREOXID]
,[PM_BAREOXNT]
,[PM_BAREPIMD]
,[PM_BAREPISC]
,[PM_BARERSST]
,[PM_BARE-RTN]
,[PM_BARE-RTP]
,[PM_BARESAPHA]
,[PM_BARE-SC1]
,[PM_BARESCOX]
,[PM_BARE-SOD]
,[PM_BARESTML]
,[PM_BARETHDC]
,[PM_BARE-VPD]
,[PM_BPSG-SC1]
,[PM_BR40NSC1]
,[PM_BR80NSC1]
,[PM_BRBEFLM1]
,[PM_BRBEHTOX]
,[PM_BRBEOXID]
,[PM_BRBROXID]
,[PM_BRBR-VPD]
,[PM_BRCG-SC1]
,[PM_BRCVBARE]
,[PM_BRCVBPSG]
,[PM_BRCVNIT1]
,[PM_BRCVNIT2]
,[PM_BRCVNITR]
,[PM_BRCVOXID]
,[PM_BRDEPISC]
,[PM_BRDFALOX]
,[PM_BRDFMTLS]
,[PM_BRDF-RTP]
,[PM_BRDF-SOD]
,[PM_BRDFTHALOX]
,[PM_BRDF-VPD]
,[PM_BRGADPN]
,[PM_BRGAISSG]
,[PM_BRGAOXID]
,[PM_BRRSNTOX]
,[PM_BRS1NTOX]
,[PM_BRSAPH-VPD]
,[PM_BRSCRSST]
,[PM_BRWPALOX]
,[PM_BRWP-SC1]
,[PM_CARBDARC]
,[PM_CARBDCST]
,[PM_CUBRBEVL]
,[PM_CUBRCUBR]
,[PM_CUBRCUOX]
,[PM_CUBR-DIF]
,[PM_CUBR-SC1]
,[PM_CUBR-VPD]
,[PM_CUCVCUBR]
,[PM_CUOXCUCU]
,[PM_CUOXCUOX]
,[PM_CUOXNDCU]
,[PM_CUOX-SC1]
,[PM_CUTSDFOXID]
,[PM_DLCDARC]
,[PM_GEBAISSG]
,[PM_GEBRCVNIT1]
,[PM_GEBRSCOX]
,[PM_HARPFLMY]
,[PM_HARPHORS]
,[PM_HARPOXID]
,[PM_HIK-VPD]
,[PM_ISSG-MTL]
,[PM_NITRRSST]
,[PM_NITR-SC1]
,[PM_OXIDBPLY]
,[PM_OXIDCGOX]
,[PM_OXIDOXCG]
,[PM_OXIDPOLY]
,[PM_RSSTFLM2]
,[PM_RSSTRSST]
,[PM_SIOXALOX]
,[PM_SIOXOXID]
,[PM_STINTUNG]
,[PM_TEOSALOX]
,[PM_TEOSMTST]
,[PM_TEOSOXID]
,[PM_TEOSTITN]
,[PM_TEOSWPST]
,[PM_TRBEOXID]
,[PM_TSDFGGOX]
,[PM_TSDFHKOX]
,[PM_TSDFOXID]
,[PV_CY-CU2FLM3]
,[PV_CY-CU-FLM3]
,[PV_NM1CCUOXNDCU]
,[PV_PM1C1KOXALCU]
,[PV_PM1N1KOXBILD]
,[PV_PM1N1KOXMTLS]
,[PV_PM1N1KOXRCLM]
,[PV_PM1N1KOXTITN]
,[PV_PM-AL-BILD]
,[PV_PM-AL-CBLD]
,[PV_PMCUBILD-C]
,[PV_PMCUBILD-D]
,[PV_PMCUBILD-E]
,[PV_PM-CU-CBLD]
,[PV_PM-CU-FLMA]
,[PV_PM-TINBILD]
,[RD_CY-GENBILD]
,[RD_GW2NBAREVIBR]
,[RD_GWKSPDFLM1]
,[RD_GWKSPDFLM2]
,[RD_GWPTRN]
,[RD_PM1NBAREBARE]
,[RD_PM-GENBILD]
,[WP_6KDCOXID]
,[WP_BAREHKBR]
,[WP_CYB100BILD]
,[WP_CYBAREDVP1]
,[WP_CYBAREDVP2]
,[WP_CYBEVLBILD]
,[WP_CYBEVLCWAC]
,[WP_CYBEVLFLM1]
,[WP_CYBOEBILD]
,[WP_CY-CU-BEVL]
,[WP_CY-CU-PLAT]
,[WP_CY-HIK-FLM1]
,[WP_CYNOCUBILD]
,[WP_CYNOCUHOOD]
,[WP_CYNOCUSCRB]
,[WP_CYNOCUSTRP]
,[WP_CY-RST BILD]
,[WP_CY-RST-PM]
,[WP_CY-RSTRS]
,[WP_CYSCRBFLM2]
,[WP_CYSCRBFLM3]
,[WP_CYSCRBFLM4]
,[WP_CYSCRBFLM5]
,[WP_CYTMAHBILD]
,[WP_CYTMAHMT-C]
,[WP_MT1NALOXETCH]
,[WP_MT1NBPSGVTCH]
,[WP_MT1NSPHASIRD]
,[WP_MT1NTEOSOXID]
,[WP_NM1N1KOXOXID]
,[WP_NM1NBAREWBVL]
,[WP_NM1NNITRNITR]
,[WP_PM1CCUBRBEVL]
,[WP_PM1CCUBRDPHO]
,[WP_PM1CCUBR-FC3]
,[WP_PM1CCUBR-SC1]
,[WP_PM1NBAREALOX]
,[WP_PM1NBAREBEVL]
,[WP_PM1NBARENF01]
,[WP_PM1NBARE-SC1]
,[WP_PM1NBAREWNSW]
,[WP_PM1NBAREWPST]
,[WP_PMBARE-SC1]
,[WP_SEADIRPIR]
,[WP_TOPOB17E]
,[WP_TOPOB27A]
,[XM_BARENO3L]
,[XP_BARENO3L]
)) u;

select * 
into #t3
from #t2
where Totalrun <> 0

select
t.WW, cast(t.Date as date) as Date, t.site, t.qual_definition, t3.TWType , (t.Totalrun * t3.Totalrun) as TotalRun
into #t4
FROM [MFG_METRICS].[MFG_METRICS].[Qual_Run_History] t
left join #t3 t3 on t.qual_definition = t3.[QUAL DEFINITION]

select 
t.WW,
t.Date,
t.site,
t.TWType,
sum(t.TotalRun) as Totalrun
into #t5
from #t4 t
where
TWType is not null
group by
t.WW,
t.Date,
t.site,
t.TWType


select 
Rtrim(cast (ww.mfg_year_no as varchar) +'-'+ case when len(cast(ww.mfg_ww_no as varchar)) = 2 then cast(ww.mfg_ww_no as varchar) else '0' +  cast(ww.mfg_ww_no as varchar) end) as WW,
d.Date,
b.TWType,
c.site
into #date2
from #date d
inner join [reference].[dbo].[mfg_year_month_ww] ww on d.Date >= ww.mfg_ww_begin_datetime and d.Date < ww.mfg_ww_end_datetime
cross join (select distinct TWType from #t4 where TWType is not null) b
cross join (select distinct site from #t4 where TWType is not null) c

select 
d.WW as WW,
d.Date as Date,
d.TWType as TWTYPE,
d.site as SITE,
case when t5.Totalrun is null then 0 else t5.Totalrun end as Totalrun
into  #final--[TWType_Count_History]
from #date2 d
left join 
#t5 t5 
on 
t5.Date = d.Date
and t5.WW = d.WW
and t5.TWType = d.TWType
and t5.site = d.site
where
d.Date <> cast(getdate() as date)
order by d.Date

DROP Table IF EXISTS [MFG_METRICS].[MFG_METRICS].[TWType_Count_History];
create table [MFG_METRICS].[MFG_METRICS].[TWType_Count_History]
(
WW              varchar(100)          NULL,
Date			date          NULL,
TWTYPE              varchar(100)          NULL,
SITE               varchar(100)          NULL,
Totalrun              int        NULL
)
insert into [MFG_METRICS].[MFG_METRICS].[TWType_Count_History]
select 
*
from #final f
where
f.WW in (
select distinct t.WW from (
select 
f.WW,
sum(f.Totalrun) as total
from #final f
group by 
f.WW
) t
where
t.total <> 0)
order by f.WW,f.Date      
       ''')

conn.commit()



################################################################################       TWTYPE Count

print("\n" + "start process " + str(datetime.datetime.now()))
print("Updating ML Delivery prod06\n")

conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD06',user='SIC_METRICS',password='SIC123')
cursor = conn.cursor()
cursor.execute('''
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
if object_id('tempdb.dbo.#view') is not null drop table #view
if object_id('tempdb.dbo.#stuff') is not null drop table #stuff

Create table #temp4
( qual_run_OID  OID     NULL,
orphan_reason varchar(1000)         NULL
)
INSERT  #temp4
(orphan_reason, qual_run_OID)
select
'Not in Reset Bridge'  ,  qr.qual_run_OID as qual_run_OID
from
fab_recipe..qual_run qr
left outer join fab_recipe..qual_xref_reset_bridge qxrb
on qr.qual_run_OID = qxrb.qual_run_OID
where
qxrb.qual_run_OID is null
INSERT  #temp4
(orphan_reason, qual_run_OID)
select
'No Qual Instance' , qr.qual_run_OID
from
fab_recipe..qual_run qr
left outer join fab_recipe..recipe_equip_group_qual_xref qi
on   qr.recipe_equip_grp_qual_xref_OID = qi.recipe_equip_grp_qual_xref_OID
where
qi.recipe_equip_grp_qual_xref_OID is null
INSERT #temp4
(orphan_reason, qual_run_OID)
select
'No Parent Qual Run Condition'  , qr.qual_run_OID
from
fab_recipe..qual_run qr
left outer join fab_recipe..qual_run_condition qrc on
qr.parent_qual_run_condition_OID = qrc.qual_run_condition_OID
where
qr.parent_qual_run_condition_OID is not null and
qrc.qual_run_condition_OID is null

select
qual_run_OID
, recipe_equip_grp_qual_xref_OID
, parent_qual_run_condition_OID
, lot_id
,qual_step
,min(Case  when qual_status = 'InProgress'  then date_time   else NULL end ) as Start_datetime
,min(Case  when qual_status = 'Failed'  then date_time   else NULL end ) as Failed_datetime
, max(Case  when qual_status in ('Failed', 'Aborted', 'Released')  then qual_status_desc else NULL end) as Fail_Description
,count(Case  when qual_status = 'InRetry'  then date_time else NULL end ) as retry_count
,count(Case  when qual_status = 'Failed'  then date_time else NULL end ) as fail_count
,min(Case  when qual_status = 'Aborted'  then date_time   else NULL end ) as Aborted_datetime
,min(Case  when qual_status = 'Released'  then date_time   else NULL end ) as Released_datetime
,max(Case  when qual_status = 'Valid'  then date_time else NULL end ) as Valid_datetime
into #temp1
FROM
fab_recipe..qual_run_event    QRE
where
start_qual_date_time  >= (select cast(cast(ww.mfg_ww_begin_datetime as date) as datetime)  from [reference].[dbo].[mfg_year_month_ww] ww where GETDATE()-14  between ww.mfg_ww_begin_datetime and ww.mfg_ww_end_datetime
) --and start_qual_date_time  < GETDATE()
and event_type_code ='INSERT'
group by
qual_run_OID
, recipe_equip_grp_qual_xref_OID
, parent_qual_run_condition_OID
, lot_id
,qual_step
 
select  distinct  T1.parent_qual_run_condition_OID , QRCE.qual_run_OID  as parent_qual_run_OID , QRCE.data_validation_step
into #temp2
FROM
#temp1  T1
INNER JOIN fab_recipe..qual_run_condition_event  QRCE
ON T1.parent_qual_run_condition_OID = QRCE.qual_run_condition_OID
select  distinct  T1.qual_run_OID , QRCE.data_validation_step  as failed_data_validation_step
into #temp3
FROM
#temp1  T1
INNER JOIN fab_recipe..qual_run_condition_event  QRCE
ON T1.qual_run_OID = QRCE.qual_run_OID
where T1.parent_qual_run_condition_OID  IS NULL   and
QRCE.qual_run_condition_status = 'Failed'

CREATE TABLE #TestWafers(
lot_id      CHAR(20) NOT NULL
,wafer_id       CHAR(20) NOT NULL
,TWType     VARCHAR(20) NULL
,wafer_OID  OID NOT NULL
,traveler VARCHAR(20) NULL
)
INSERT INTO
#TestWafers
SELECT DISTINCT
ws.lot_id
,ws.wafer_id
,MAX(CASE WHEN CITEM.corr_item_desc = 'WLA TWTYPE' THEN wa.wafer_attr_value ELSE NULL END) AS twtype
,ws.wafer_OID
,TRAV.trav_id
FROM
traveler..corr_item CITEM
INNER JOIN fab_lot_extraction..wafer_attr wa
ON CITEM.corr_item_OID = wa.corr_item_OID
INNER JOIN fab_lot_extraction..wafer_status ws
ON wa.wafer_OID = ws.wafer_OID
INNER JOIN fab_lot_extraction..fab_lot_status ls
ON ws.lot_id = ls.lot_id
INNER JOIN #temp1 t
ON ls.lot_id = t.lot_id
INNER JOIN traveler..trav_step ts ON
ls.trav_step_OID = ts.trav_step_OID
INNER JOIN traveler..step s ON
ts.step_OID = s.step_OID
INNER JOIN traveler..traveler TRAV ON
ts.trav_OID = TRAV.trav_OID
WHERE
CITEM.corr_item_type = 'WAFER ATTRIBUTE'
AND RTRIM(CITEM.corr_item_desc) IN ('WLA TWTYPE','WLA STORAGE LOCATION','WLA TWACTION','WLA TWCONTAMINATION')
AND wa.wafer_attr_value IS NOT NULL
AND ws.wafer_state <> 'Scrap'
AND (ls.state_code < 'Complete' OR ls.state_code > 'Complete')
AND ls.lot_current_qty > 0
GROUP BY
ws.lot_id
,ws.wafer_id
,ws.wafer_OID
,TRAV.trav_id

--select 
--TW.lot_id,
--TW.traveler,
--TW.TWType,
--COUNT(TW.wafer_OID) as QTY
--into #QTY
--from #TestWafers TW
--group by 
--TW.lot_id,
--TW.traveler,
--TW.TWType

Create table #finale
(
area_id         varchar(3000)          NULL,
WS_Group               varchar(3000)          NULL,
chamber               varchar(3000)          NULL,
qual_definition                varchar(3000)          NULL,
lot_id                varchar(2000)         NULL,
qual_step                varchar(4000)          NULL,
Start_datetime          datetime          NULL,
Failed_datetime         datetime          NULL,
start_qual_date_time      datetime          NULL,
data_validation_step      varchar(4000)          NULL,
Fail_Description                varchar(4000)          NULL,
fail_count              varchar(4000)      NULL,
retry_count             varchar(4000)      NULL,
Qual_Result_datetime            datetime          NULL,
Qual_Result               varchar(4000)          NULL,
TWType               varchar(4000)          NULL,
traveler            varchar(4000)          NULL,
qual_status         varchar(4000)          NULL,
)
Insert into #finale
select
--master.dbo.fn_mt_binarytohexstring(t.qual_run_OID )
AMA.area_id,
rtrim(FP_WSG.WS_group_name) as WS_Group
--,isnull(PEQ.equip_id,EQ.equip_id) as Tool
,EQ.equip_id as chamber
,rtrim(QD.qual_definition_name) as qual_definition
--,'N'   as Tool_qual
,t.lot_id
,t.qual_step
,Start_datetime
,Failed_datetime
,start_qual_date_time,
--,datediff(dd,'12/30/1899',Start_datetime   )
--,convert(char(8),Start_datetime   ,108)
--,datediff(dd,'12/30/1899',Failed_datetime )
--,convert(char(8),Failed_datetime ,108)
(Case when T2.data_validation_step IS NOT NULL  then T2.data_validation_step
else  T3.failed_data_validation_step 
end) as data_validation_step
, Fail_Description
, fail_count
, retry_count
, Case when Valid_datetime  IS NOT NULL then Valid_datetime
when Released_datetime IS NOT NULL then Released_datetime
when Aborted_datetime IS NOT NULL Then Aborted_datetime
else NULL
end as Qual_Result_datetime
,
Case when Valid_datetime  IS NOT NULL then 'Valid'
when Released_datetime IS NOT NULL then 'Released'
when Aborted_datetime IS NOT NULL Then 'Aborted'
when  t4.orphan_reason IS NOT NULL Then 'Orphaned'
when  qr.qual_run_OID   IS NOT NULL then   'Still Active'
else  'ClosedByParent'
end as Qual_Result
--,       t4.orphan_reason
----,        Case when t.parent_qual_run_condition_OID is NOT NULL then 'Sub Qual' ELSE 'Parent Qual'  END as Parent/Sub
----,     Case when (Case when Valid_datetime  IS NOT NULL then Valid_datetime
----when Released_datetime IS NOT NULL then Released_datetime
----when Aborted_datetime IS NOT NULL Then Aborted_datetime
----else NULL
----end) IS NULL Then NULL
----ELSE datediff(mi,  Start_datetime , (Case when Valid_datetime  IS NOT NULL then Valid_datetime
----when Released_datetime IS NOT NULL then Released_datetime
----when Aborted_datetime IS NOT NULL Then Aborted_datetime
----else NULL
----end))
----END as run_duration_mins
--,t.qual_run_OID
--, t.recipe_equip_grp_qual_xref_OID
--, Case when T2.parent_qual_run_OID  IS NULL then t.qual_run_OID  ELSE T2.parent_qual_run_OID END as parent_qual_run_OID
,TW.TWType 
,TW.traveler
,qr.qual_status
--,datediff(hh, (Case when Valid_datetime  IS NOT NULL then NULL
--when Released_datetime IS NOT NULL then NULL
--when Aborted_datetime IS NOT NULL then NULL
--when t4.orphan_reason IS NOT NULL then Start_datetime
--when qr.qual_run_OID   IS NOT NULL then Start_datetime
--else NULL
--end), getdate())  as active_time_hrs
from #temp1 t
LEFT OUTER JOIN fab_recipe..qual_run  qr
ON t.qual_run_OID = qr.qual_run_OID
LEFT OUTER JOIN #temp4  t4
ON qr.qual_run_OID   = t4.qual_run_OID
LEFT OUTER JOIN #temp2 T2
ON t.parent_qual_run_condition_OID = T2.parent_qual_run_condition_OID
LEFT OUTER JOIN #temp3 T3
ON t.qual_run_OID = T3.qual_run_OID
LEFT OUTER JOIN  fab_recipe..recipe_equip_group_qual_xref AS REGQX
ON t.recipe_equip_grp_qual_xref_OID = REGQX.recipe_equip_grp_qual_xref_OID
LEFT OUTER JOIN fab_recipe..qual_definition AS QD
ON REGQX.qual_definition_OID = QD.qual_definition_OID
LEFT OUTER JOIN fab_recipe..aggregate_mfg_area AS AMA
ON QD.area_OID = AMA.area_OID
LEFT OUTER JOIN fab_recipe..recipe_equip_group AS REG
ON REGQX.recipe_equip_group_OID = REG.recipe_equip_group_OID
LEFT OUTER JOIN fab_recipe..equip_group AS EG
ON REG.equipment_group_OID = EG.equipment_group_OID
LEFT OUTER JOIN fab_recipe..equip_for_equip_group AS EFEG
ON REG.equipment_group_OID = EFEG.equipment_group_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment AS EQ
ON EFEG.equipment_OID = EQ.equip_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment_cluster CLSTR
ON EQ.equip_OID = CLSTR.child_equip_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment PEQ
ON CLSTR.parent_equip_OID = PEQ.equip_OID
LEFT OUTER JOIN reference..FP_equip FP_EQ
ON EQ.equip_id = FP_EQ.equip_name
LEFT OUTER JOIN reference..FP_WS FP_WS
ON FP_EQ.WS_OID = FP_WS.WS_OID
LEFT OUTER JOIN reference..FP_WS_in_WS_group FP_WSIG
ON FP_WS.WS_OID  = FP_WSIG.WS_OID
LEFT OUTER JOIN reference..FP_WS_group  FP_WSG
ON FP_WSIG.WS_group_OID = FP_WSG.WS_group_OID
LEFT OUTER JOIN #TestWafers TW
ON t.lot_id = TW.lot_id
WHERE
(FP_WSG.WS_group_type = 'Scheduling'
--OR
--FP_WSG.WS_group_name IS NULL
)
union ALL
SELECT
--master.dbo.fn_mt_binarytohexstring(t.qual_run_OID )
AMA.area_id,
rtrim(FP_WSG.WS_group_name) as WS_Group
--,isnull(PEQ.equip_id,EQ.equip_id) as Tool
,EQ.equip_id as chamber
,rtrim(QD.qual_definition_name) as qual_definition
--,'N'   as Tool_qual
,t.lot_id
,t.qual_step
,Start_datetime
,Failed_datetime
,start_qual_date_time,
--,datediff(dd,'12/30/1899',Start_datetime   )
--,convert(char(8),Start_datetime   ,108)
--,datediff(dd,'12/30/1899',Failed_datetime )
--,convert(char(8),Failed_datetime ,108)
(Case when T2.data_validation_step IS NOT NULL  then T2.data_validation_step
else  T3.failed_data_validation_step 
end) as data_validation_step
, Fail_Description
, fail_count
, retry_count
, Case when Valid_datetime  IS NOT NULL then Valid_datetime
when Released_datetime IS NOT NULL then Released_datetime
when Aborted_datetime IS NOT NULL Then Aborted_datetime
else NULL
end as Qual_Result_datetime
,
Case when Valid_datetime  IS NOT NULL then 'Valid'
when Released_datetime IS NOT NULL then 'Released'
when Aborted_datetime IS NOT NULL Then 'Aborted'
when  t4.orphan_reason IS NOT NULL Then 'Orphaned'
when  qr.qual_run_OID   IS NOT NULL then   'Still Active'
else  'ClosedByParent'
end as Qual_Result
--,       t4.orphan_reason
----,        Case when t.parent_qual_run_condition_OID is NOT NULL then 'Sub Qual' ELSE 'Parent Qual'  END as Parent/Sub
----,     Case when (Case when Valid_datetime  IS NOT NULL then Valid_datetime
----when Released_datetime IS NOT NULL then Released_datetime
----when Aborted_datetime IS NOT NULL Then Aborted_datetime
----else NULL
----end) IS NULL Then NULL
----ELSE datediff(mi,  Start_datetime , (Case when Valid_datetime  IS NOT NULL then Valid_datetime
----when Released_datetime IS NOT NULL then Released_datetime
----when Aborted_datetime IS NOT NULL Then Aborted_datetime
----else NULL
----end))
----END as run_duration_mins
--,t.qual_run_OID
--, t.recipe_equip_grp_qual_xref_OID
--, Case when T2.parent_qual_run_OID  IS NULL then t.qual_run_OID  ELSE T2.parent_qual_run_OID END as parent_qual_run_OID
,TW.TWType 
,TW.traveler
,qr.qual_status
--,datediff(hh, (Case when Valid_datetime  IS NOT NULL then NULL
--when Released_datetime IS NOT NULL then NULL
--when Aborted_datetime IS NOT NULL then NULL
--when t4.orphan_reason IS NOT NULL then Start_datetime
--when qr.qual_run_OID   IS NOT NULL then Start_datetime
--else NULL
--end), getdate())  as active_time_hrs
from #temp1 t
LEFT OUTER JOIN fab_recipe..qual_run  qr
ON t.qual_run_OID = qr.qual_run_OID
LEFT OUTER JOIN #temp4  t4
ON qr.qual_run_OID   = t4.qual_run_OID
LEFT OUTER JOIN #temp2 T2
ON t.parent_qual_run_condition_OID = T2.parent_qual_run_condition_OID
LEFT OUTER JOIN #temp3 T3
ON t.qual_run_OID = T3.qual_run_OID
LEFT OUTER JOIN  fab_recipe..recipe_equip_group_qual_xref AS REGQX
ON t.recipe_equip_grp_qual_xref_OID = REGQX.recipe_equip_grp_qual_xref_OID
LEFT OUTER JOIN fab_recipe..qual_definition AS QD
ON REGQX.qual_definition_OID = QD.qual_definition_OID
LEFT OUTER JOIN fab_recipe..aggregate_mfg_area AS AMA
ON QD.area_OID = AMA.area_OID
LEFT OUTER JOIN fab_recipe..equip_group AS EG
ON REGQX.recipe_equip_group_OID = EG.equipment_group_OID
LEFT OUTER JOIN fab_recipe..equip_for_equip_group AS EFEG
ON EG.equipment_group_OID = EFEG.equipment_group_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment AS EQ
ON EFEG.equipment_OID = EQ.equip_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment_cluster CLSTR
ON EQ.equip_OID = CLSTR.child_equip_OID
LEFT OUTER JOIN equip_tracking_DSS..equipment PEQ
ON CLSTR.parent_equip_OID = PEQ.equip_OID
LEFT OUTER JOIN reference..FP_equip FP_EQ
ON EQ.equip_id = FP_EQ.equip_name
LEFT OUTER JOIN reference..FP_WS FP_WS
ON FP_EQ.WS_OID = FP_WS.WS_OID
LEFT OUTER JOIN reference..FP_WS_in_WS_group FP_WSIG
ON FP_WS.WS_OID  = FP_WSIG.WS_OID
LEFT OUTER JOIN reference..FP_WS_group  FP_WSG
ON FP_WSIG.WS_group_OID = FP_WSG.WS_group_OID
LEFT OUTER JOIN #TestWafers TW
ON t.lot_id = TW.lot_id
WHERE
(FP_WSG.WS_group_type = 'Scheduling'
)

DROP Table IF EXISTS [SIC_METRICS].[SIC_METRICS].[ML_DeliveryTime];
create table [SIC_METRICS].[SIC_METRICS].[ML_DeliveryTime]
(
WW              varchar(100)          NULL,
Date			date          NULL,
site              varchar(100)          NULL,
qual_definition              varchar(100)          NULL,
TWType              varchar(100)          NULL,
chamber              varchar(100)          NULL,
lot_id              varchar(100)          NULL
)
insert into [SIC_METRICS].[SIC_METRICS].[ML_DeliveryTime]
select distinct
Rtrim(cast (ww.mfg_year_no as varchar) +'-'+ case when len(cast(ww.mfg_ww_no as varchar)) = 2 then cast(ww.mfg_ww_no as varchar) else '0' +  cast(ww.mfg_ww_no as varchar) end) as WW,
cast((case when f.Start_datetime is null then f.start_qual_date_time else f.Start_datetime end ) as date) as Date,
case when 
substring(chamber,7,2) like '[0-9][A-Z]' then 'F10A' when
substring(chamber,7,2) like '[A-Z][A-Z]' then 'F10X' when
substring(chamber,7,2) like '[0-9][0-9]' then 'F10N' when
substring(chamber,7,2) like '[A-Z][0-9]' then 'F10N' end as [site],
f.qual_definition,
f.TWType,
f.chamber,
rtrim(f.lot_id) as lot_id
from #finale f
inner join [reference].[dbo].[mfg_year_month_ww] ww on 
cast(f.Start_datetime as date) > cast(ww.mfg_ww_begin_datetime as date) and 
cast(f.Start_datetime as date) <= cast (ww.mfg_ww_end_datetime as date) and
f.TWType is not null
       ''')

conn.commit()


print("\n" + "Finish process " + str(datetime.datetime.now()) + "\n")