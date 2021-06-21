# importing required libraries
import warnings
import itertools
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import datetime as datetime
import pyodbc 
import datetime
from numpy import loadtxt
import urllib
import sqlalchemy


# =============================================================================
# from numpy import loadtxt
# from keras.models import Sequential
# from keras.layers import Dense
# from pandas import ExcelWriter
# from pandas import ExcelFile
# 
# 
# from sklearn.model_selection import train_test_split
# from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
# from sklearn.model_selection import train_test_split # Import train_test_split function
# from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
# from sklearn.linear_model import LinearRegression
# 
# from sklearn import preprocessing
# from sklearn.preprocessing import OneHotEncoder
# =============================================================================
start = datetime.datetime.now();
print("Machine Learning Model Start " + str(start) + "\n")
plt.style.use('fivethirtyeight')

pd.reset_option('max_rows')


def main(df,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema):
    connection_string= "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;" % (SQL_server, Database, SQL_user, SQL_pwd)
    connection_string = urllib.parse.quote_plus(connection_string)
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name_in_SQL,engine,schema =schema, if_exists='replace', chunksize=200, index = False) #replace or append
    

def Time(md):
    conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD170',user='f10prod',password='Micron123')
    cursor = conn.cursor()
    Future_q = pd.read_sql_query('''
    declare @StartDate datetime = (?);
    set @StartDate = @StartDate + 1
    
    
    ;WITH cte AS (
        SELECT @StartDate AS Date
        UNION ALL
        SELECT DATEADD(day,1,Date) as Date
        FROM cte
        WHERE DATEADD(day,1,Date) <=  @StartDate + 6
    )
    SELECT cast( Date as date) as Date
    FROM cte
    OPTION (MAXRECURSION 0)
    ''',conn, params=[Dataset['Date'].max()])
    Future=pd.DataFrame(Future_q)
    return Future
    

def std(x):
    elements = np.array(x['TC'])
    mean = np.mean(elements, axis=0)
    sd = np.std(elements, axis=0)
    
    filter1 = (x['TC'] > mean - sd * 2)
    filter2 = (x['TC'] < mean + sd * 2)
    
    x = x[filter1]
    x = x[filter2]
    
    return x, sd

#print(mean - sd )
#print(mean + sd )



def Assign(assign,count):

    assigno = assign    
    
    if int( 25 * round( assign/ 25. )) < assign:
        assign =  int( 25 * round( assign/ 25. )) + 25
    else:
        assign = int( 25 * round( assign/ 25. ))
        
    assign= assign/count
    
    assign =  int( 25 * round( assign/ 25. )) 
    
    extra = assigno - (assign * count)
    
    if extra <= 0:
        extra = 0
    else:
        extra = 25
        
    if assign < 0:
        assign = 0        
    
    return assign,extra
        


def ML(ts,twtype,fab):
    
    #pd.set_option('display.max_rows', 10000)
    #ts =  tw83A
    #twtype = unique[0]
    #fab = fab[0]
#    print(twtype)
#    print(fab)

    tso = ts.copy()

    #print(ts)
    #print(tso)
    
    ts, buffer = std(ts)
    
    if ts.empty:
        ts = tso.copy()
#        print('DataFrame is empty!')
        
    mindate = min(ts['Date'])
    maxdate = max(ts['Date']) 
    
    tso.set_index('Date', inplace=True)           
    #del tso.index.name
    tso.index.name = None
    
    ts.set_index('Date', inplace=True)           
    #del ts.index.name
    ts.index.name = None
    
    
    
    # The 'MS' string groups the data in buckets by start of the month
    
    
    # The term bfill means that we use the value before filling in missing values
    # The 'MS' string groups the data in buckets by start of the month
    
    #ts = ts['WW'].resample('MS').mean()
    #ts = ts.fillna(ts.bfill())
    
    
 
# =============================================================================
#     x = ts.plot(figsize=(10, 6)) 
#     x.set_xlabel('Date')
#     x.set_ylabel('TWType usage')
#     plt.show()
#     
# =============================================================================

    
    
    #step 4
    # Define the p, d and q parameters to take any value between 0 and 2
    p = d = q = range(0, 2)
    
    # Generate all different combinations of p, q and q triplets
    pdq = list(itertools.product(p, d, q))
    
    # Generate all different combinations of seasonal p, q and q triplets
    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]
    
# =============================================================================
#     print('Examples of parameter combinations for Seasonal ARIMA...')
#     print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[1]))
#     print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[2]))
#     print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[3]))
#     print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[4]))
#     
# =============================================================================
    
    #Desirable Dataset
    warnings.filterwarnings("ignore") # specify to ignore warning messages
    AICcheck = 10000.0
    Targetparam = ()
    Targetsparam = ()
    for param in pdq:
        for param_seasonal in seasonal_pdq:
            try:
                mod = sm.tsa.statespace.SARIMAX(ts,
                                                order=param,
                                                seasonal_order=param_seasonal,
                                                enforce_stationarity=False,
                                                enforce_invertibility=False)
    
                results = mod.fit()
    
                #print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
                if (results.aic < AICcheck) and (results.aic > -4):
                    AICcheck = results.aic
                    Targetparam = param
                    Targetsparam = param_seasonal
                    #AICcheck.append(results.aic)
            except:
                continue
            

 
         
    #step 5 
#    print('choosen AIC: ' +str(AICcheck))
#    print('choosen param: ' +str(Targetparam))
#    print('choosen sparam: ' +str(Targetsparam))
    
    
    #Not Desirable Dataset (pick min)    
    if (Targetparam == () ) or (Targetsparam == () ):
        
#        print('Skip')
        maxdate = datetime.datetime.strptime(maxdate, '%Y-%m-%d')
        dz = {'Date' : [ (maxdate + datetime.timedelta(1)), (maxdate + datetime.timedelta(2)), (maxdate + datetime.timedelta(3)), (maxdate + datetime.timedelta(4)), (maxdate + datetime.timedelta(5)), (maxdate + datetime.timedelta(6)), (maxdate + datetime.timedelta(7)) ], 'TC':[0,0,0,0,0,0,0]}
        Full_result= pd.DataFrame( data=dz )
        roundedmean = 0
        
    else:
        
        mod = sm.tsa.statespace.SARIMAX(ts, trend='c',
                                        #order=(1, 1, 1),
                                        #seasonal_order=(1, 1, 1, 12)
                                        order=Targetparam,
                                        seasonal_order=param_seasonal
                                        ,enforce_stationarity=False
                                        ,enforce_invertibility=False
                                        )
        
        #mod = pm.auto_arima(ts,seasonal=True, m=12)
        
        results = mod.fit()
        
        #print(results.summary().tables[1])
        '''
        results.plot_diagnostics(figsize=(15, 12))
        plt.show()
        
        '''
        
        
        #step 6
        #pred = results.get_prediction(start=pd.to_datetime('2020-25' + '0', format='%Y-%W%w'), dynamic=False)
        pred = results.get_prediction(start=mindate, dynamic=False)
        pred_ci = pred.conf_int()
        
    # =============================================================================
    #     ax = ts[week:].plot(label='observed')
    #     pred.predicted_mean.plot(ax=ax, label='One-step ahead Forecast', alpha=.7)
    #     
    #     ax.fill_between(pred_ci.index,
    #                     pred_ci.iloc[:, 0],
    #                     pred_ci.iloc[:, 1], color='k', alpha=.2)
    # =============================================================================
        
    # =============================================================================
    #     ax.set_xlabel('Date')
    #     ax.set_ylabel('TWType usage')
    #     plt.legend()
    #     
    #     plt.show()
    # =============================================================================
        
        
        y_forecasted = pred.predicted_mean
        y_truth = ts.loc[mindate:]['TC']
        # Compute the mean square error
        mse = ((y_forecasted - y_truth) ** 2).mean()
#        print('The Mean Squared Error of our model is {}'.format(round(mse, 2)))
        
        
        #######
        pred_dynamic = results.get_prediction(start=mindate, dynamic=True, full_results=True)
        pred_dynamic_ci = pred_dynamic.conf_int()
        
    # =============================================================================
    #     ax = ts[week:].plot(label='observed', figsize=(12, 6))
    #     pred_dynamic.predicted_mean.plot(label='Dynamic Forecast', ax=ax)
    #     
    #     ax.fill_between(pred_dynamic_ci.index,
    #                     pred_dynamic_ci.iloc[:, 0],
    #                     pred_dynamic_ci.iloc[:, 1], color='k', alpha=.25)
    #     
    #     ax.fill_betweenx(ax.get_ylim(), week, ts.index[-1],
    #                      alpha=.1, zorder=-1)
    #     
    # =============================================================================
    # =============================================================================
    #     ax.set_xlabel('Date')
    #     ax.set_ylabel('TWType usage')
    #     plt.legend()
    #     plt.show()
    #     
    # =============================================================================
        
        
        
        y_forecasted = pred_dynamic.predicted_mean
        y_truth = ts.loc[mindate:]['TC']
        
        
        
        
        # Compute the mean square error
        mse = ((y_forecasted - y_truth) ** 2).mean()
#        print('The Mean Squared Error of our model is {}'.format(round(mse, 2)))
        
        
        #step 7
        # Get forecast 500 steps ahead in future
        pred_uc = results.get_forecast(steps=7)
        
        # Get confidence intervals of forecasts
        pred_ci = pred_uc.conf_int()
        
#        ax = ts.plot(label='observed', figsize=(12, 6))
#        pred_uc.predicted_mean.plot(ax=ax, label='Forecast')
#        ax.fill_between(pred_ci.index,
#                        pred_ci.iloc[:, 0],
#                        pred_ci.iloc[:, 1], color='k', alpha=.25)
#        ax.set_xlabel('Date')
#        ax.set_ylabel('TWType usage')
        
        #ax.set_xlim([xmin,xmax])
        #ax.set_ylim([0,1500])
#        plt.legend()
#        plt.show()
        
        
        Forecast_result = (pred_ci.iloc[:, 0] + pred_ci.iloc[:, 1])/2
        #print(pred_ci.iloc[:, 0])
        #print(pred_ci.iloc[:, 1])
        a = Future.reset_index(drop=True)
        b = Forecast_result.round().astype(int).rename("TC").reset_index(drop=True)
        roundedmean = round(b.mean())

        
        Full_result = pd.concat([a,b],axis=1)
        Full_result = Full_result[['Date','TC']].dropna()


#buffer    
    bufferassign = 0 # round((buffer*2).mean())
    d = {'TWType': [twtype],'Site': [fab],'Assign': [roundedmean+bufferassign]}
    mean = pd.DataFrame(data=d)

    tso = tso.reset_index()
    tso = tso.rename(columns={"index":"Date"})
    tsq = tso[['Date','TC']]
    #print(mean)
    #del ts['level_0']#ts = ts.rename(columns={'index':'WW'}).reset_index(drop=True)
    Full_results = pd.concat([tsq,Full_result])
    #print(Full_results)
    return Full_results, mean

##################################################################            End of function

#TWTYPE USAGE DATASET
conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD170',user='f10prod',password='Micron123')
cursor = conn.cursor()
Dataset_q = pd.read_sql_query('''
set transaction isolation level Read Uncommitted
SET NOCOUNT ON
Declare @Min int
if object_id('tempdb.dbo.#include') is not null drop table #include
if object_id('tempdb.dbo.#major') is not null drop table #major

Set @Min = 
(select 
round(count(distinct WW)*0.8,1)
FROM [MFG_METRICS].[MFG_METRICS].[TWType_Count_History]
)

select *
into #include
from 
(
select 
q.TWTYPE,
count(distinct q.WW) as count
from [MFG_METRICS].[MFG_METRICS].[TWType_Count_History] q
where
q.Totalrun <> 0 -- and TWTYPE like '%DE_CN-RSTRG01%'
group by
q.TWTYPE
) q
where
q.count >= @Min

select --top 3
q.TWTYPE,
SUM(q.Totalrun) as MAJOR
into #major
from  [MFG_METRICS].[MFG_METRICS].[TWType_Count_History] q
join #include i on i.TWTYPE = q.TWTYPE
group by
q.TWTYPE
order by SUM(q.Totalrun) desc



select 
q.Date,
q.TWTYPE,
q.SITE,
sum(q.Totalrun) as TC
FROM [MFG_METRICS].[MFG_METRICS].[TWType_Count_History] q
join #include i on i.TWTYPE = q.TWTYPE
where
q.TWTYPE in (
select distinct m.TWTYPE from #major m
)
group by
q.Date,
q.TWTYPE,
q.SITE

''',conn)
Dataset=pd.DataFrame(Dataset_q)


unique = Dataset['TWTYPE'].unique()
TWTYPE = "("
for x in unique:
    TWTYPE = TWTYPE + "'" + x + "',"
    
TWTYPE = TWTYPE[:-1]
TWTYPE = TWTYPE + ")"
print("LIST OF TWTYPE: " + TWTYPE)


#DELIVERY DATA
conn = pyodbc.connect(Driver='SQL Server',server='FSMSSPROD170',user='f10prod',password='Micron123')
cursor = conn.cursor()
Delivery_q = pd.read_sql_query('''
SELECT * 
FROM [MFG_METRICS].[MFG_METRICS].[Delivery_info]  d

--where
--------------------------------------------------------------------------------------------------------------------ML --PARAMETER
--d.TWType  in  ('MT_6KDCOXID','MT_BAREHARP','MT_BRBEOXID')  
-------------------------------------------------------------------------------------------------------------------------------

''',conn)
Delivery_d=pd.DataFrame(Delivery_q)





##########################################################          MAIN


unique = Dataset['TWTYPE'].unique()
fab = ('F10N','F10X','F10A')

#week = '2020-10'
#Dataset

#fab[0]
#unique[1]


Future = Time(Dataset['Date'].max())


for L in range(len(unique)):
   
    k = L + 1
    exec(f'tw{k}N = Dataset[(Dataset.TWTYPE == unique[{L}]) & (Dataset.SITE == fab[0])]')
    exec(f"tw{k}N = tw{k}N.filter(items=['Date','TC'])")
    #exec('print(tw%sN)' % (k) ) 



    exec(f'tw{k}X = Dataset[(Dataset.TWTYPE == unique[{L}]) & (Dataset.SITE == fab[1])]')
    exec(f"tw{k}X = tw{k}X.filter(items=['Date','TC'])")
    #exec('print(tw%sX)' % (k) ) 

    
    exec(f'tw{k}A = Dataset[(Dataset.TWTYPE == unique[{L}]) & (Dataset.SITE == fab[2])]')
    exec(f"tw{k}A = tw{k}A.filter(items=['Date','TC'])")
    #exec('print(tw%sA)' % (k) ) 

    

for L in range(len(unique)):
    
    k = L + 1
    exec(f'tw{k}N, mean{k}N = ML(tw{k}N,unique[{L}],fab[0])')
    exec(f'tw{k}N = tw{k}N.rename(columns={{"TC":fab[0]}})')
    exec(f"tw{k}N['TWTYPE'] = unique[{L}]")
    

    
    exec(f'tw{k}X, mean{k}X = ML(tw{k}X,unique[{L}],fab[1])')
    exec(f'tw{k}X = tw{k}X.rename(columns={{"TC":fab[1]}})')
    exec(f"tw{k}X['TWTYPE'] = unique[{L}]")
    
    
    exec(f'tw{k}A, mean{k}A = ML(tw{k}A,unique[{L}],fab[2])')
    exec(f'tw{k}A = tw{k}A.rename(columns={{"TC":fab[2]}})')
    exec(f"tw{k}A['TWTYPE'] = unique[{L}]")
    
    exec(f"result{k} = tw{k}N.merge(tw{k}X.set_index('Date'), how='inner',  left_on=[\"Date\", \"TWTYPE\"], right_on=[\"Date\", \"TWTYPE\"])")
    exec(f"result{k} = result{k}.merge(tw{k}A.set_index('Date'), how='inner', left_on=[\"Date\", \"TWTYPE\"], right_on=[\"Date\", \"TWTYPE\"])")
    exec(f"result{k} = result{k}[['Date','TWTYPE','F10N','F10X','F10A']]")

    
    



dfresult = ""
for L in range(len(unique)):
    
    k = L + 1
    exec(f"dfresult = dfresult + 'result' + str({k}) + ','")
    
dfmean = ""
for L in range(len(unique)):
    
    k = L + 1
    exec(f"dfmean = dfmean + 'mean' + str({k}) + 'N,' + 'mean' + str({k}) + 'X,' + 'mean' + str({k}) + 'A,'  ")



#ts = ts.dropna()
#ts['WW'] = pd.to_datetime(ts['WW'] + '0', format='%Y-%W%w')   

dfresult = dfresult[:-1]
exec('frames = [%s]' % (dfresult) )
#all union for TWTYPEs
#frames = [result,result2,result3]
finalresult = pd.concat(frames)
finalresult = finalresult.reset_index(drop=True)
finalresult.to_csv(r'H:\IMFS\Temp\Username\H\Hendy\BWS ML\Outout_Date.csv')


dfmean = dfmean[:-1]
exec('combineassign =  [%s]' % (dfmean) )
#combineassign = [mean1N,mean1X,mean1A,mean2N,mean2X,mean2A,mean3N,mean3X,mean3A]
decision = pd.concat(combineassign)
decision = decision.reset_index(drop=True)


print(finalresult)

#Assign of ML
#print(decision)



ML_combine = decision.merge(Delivery_d, how='inner', left_on=["TWType", "Site"], right_on=["TWType", "chamber_site"])
ML_combine = ML_combine[['TWType','Site','Assign','Source_devices','capacity']]
ML_combine['Count'] = ML_combine['Source_devices'].str.count(',') + 1



ML_combine['Recomended'] = ML_combine['capacity']



for ind in ML_combine.index: 
     
    merge= ""
    assign = 0
    extra = 0
    form = 0
    assign,extra = Assign(ML_combine['Assign'][ind], ML_combine['Count'][ind])
    
    for form in range(ML_combine['Count'][ind]):
        merge = merge + str(assign + extra) + ','
        extra = 0
        form = form + 1
        
    ML_combine['Recomended'][ind] = merge[:-1]
    
    
ML_combine = ML_combine[['TWType','Site','Assign','Source_devices','capacity','Recomended']]




pd.reset_option('max_rows')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
#print(ML_combine)
#ML_combine.to_csv(r'H:\IMFS\Temp\Username\H\Hendy\BWS ML\MLModelresult.csv')


pd.reset_option('max_rows')



#result to prod170
SQL_server = 'FSMSSPROD170,54059' #replace port number, check using below code
Database = 'MFG_METRICS'
SQL_user ='f10prod'
SQL_pwd = 'Micron123'
schema = 'MFG_METRICS'


table_name_in_SQL = 'MLModelresult' #replace SQLTABLETOUPLOAD
main(ML_combine,table_name_in_SQL,SQL_server,Database,SQL_user,SQL_pwd,schema) #replace DATATABLEFROMPYTHON




end = datetime.datetime.now()
print("\n" + "Finish process " + str(datetime.datetime.now()) + " Process time: " + str((end - start).total_seconds())   + "\n")




#np.where(unique=='MT_BAREDARC')
