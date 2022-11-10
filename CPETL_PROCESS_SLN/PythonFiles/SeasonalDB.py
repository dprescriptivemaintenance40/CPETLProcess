from typing import final
import sys
import pyodbc as odbc
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.ar_model import AutoReg
import numpy as np
from datetime import date, datetime
bigdata = pd.DataFrame()

conn = odbc.connect('Driver=SQL SERVER;Server=DESKTOP-5GL6DEM;Database=DPM;Integrated Security=True')

cursor = conn.cursor()
# batchId=1
# AssetName="CentrifugalCompressor"
batchId=int(sys.argv[1])

query='''SELECT * FROM CentrifugalProcessedTables WHERE CPId={}'''
series = pd.read_sql(query.format(batchId),conn, parse_dates=['Date'])
startdate=min(series["Date"])
series.set_index('Date')
    # Step 3 :- 
series.index=pd.to_datetime(series.index, unit='D',origin=startdate)
    # Step 4 :- Season decomponse it in to observer , seasonal , trend
for col in series.columns:
  print("Col name" + col)
  if not col=="Date":
    if not col=="Id":
      if not col=="CPId":
                result=seasonal_decompose(series[col], model='additive', period=365,extrapolate_trend='freq')
                resultFinal = pd.concat([result.observed, result.seasonal,result.trend,result.resid],axis=1)
                resultFinal.index.name  = col+"Date"
                # Extract the day and month wise Seasonal and residal
                # Step 5 :- Added the totalunknow=seasonal + residual
                #resultFinal[col+'resses'] =result.seasonal+result.resid
                resultFinal[col+'resses'] =result.seasonal+result.resid
                resultFinal[col+'datemonth']  = resultFinal.index.strftime("%m/%d")
                datemonth = pd.DataFrame(columns = [col+'datemonth',col+'average'])
                datemonth[col+'datemonth'] = resultFinal[col+'datemonth']
                # Step 6 :0 datemonth is having date and month with averag
                dtmean=resultFinal.groupby([col+'datemonth'])[col+'resses'].mean()
                #print(dtmean)
                #plt.show()
                # Predict using auto regression
                Training = resultFinal[col].copy()
                model = AutoReg(Training, lags=3)
                model_fit = model.fit()
                predictedValue = model_fit.predict(0, len(resultFinal)+400)

                #predictedValue["finalvalue"]=0
                #predictedValue['datemonth']  = predictedValue.iloc[:, [0]].strftime("%m/%d")
                panda1 = pd.DataFrame({col+'Date':predictedValue.index,col+ 'ValuePredicted':predictedValue.values})
                panda1[col+"datemonth"]= panda1[col+"Date"].dt.strftime("%m/%d")

                finalDf = pd.merge(pd.DataFrame(panda1), pd.DataFrame(dtmean), left_on=[col+'datemonth'], 
                            right_on= [col+'datemonth'], how='left')
                # value predited from autoregression plus unknows
                # Add the auression + Seasonal + Resid
                finalDf[col+"New"] = finalDf[col+"ValuePredicted"] + finalDf[col+"resses"]
            
                finalDf.set_index(col+'Date', inplace=True)
                bigdata[col]=finalDf[col+"New"]
for i, row in bigdata.iterrows():
      if pd.isnull(row['Vibration3H']):
         row['Vibration3H']=0
      SQLCommand = "INSERT INTO CentrifugalPredictedTables(CPId,Date,Vibration3H) VALUES('" + str(batchId) + "','" + str(i) + "','" + str(row['Vibration3H']) + "')"
      cursor.execute(SQLCommand)
conn.commit()

