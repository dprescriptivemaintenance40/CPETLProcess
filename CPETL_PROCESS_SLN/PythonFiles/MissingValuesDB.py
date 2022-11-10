import sys
import pandas as pd
import pyodbc as odbc


conn = odbc.connect('Driver=SQL SERVER;Server=DESKTOP-5GL6DEM;Database=DPM;Integrated Security=True')

cursor = conn.cursor()
batchId=int(sys.argv[1])
print(batchId)
# batchId=2
# AssetName="CentrifugalCompressor"

#ScrewParamter
query='''SELECT * FROM CentrifugalCleaningTables2 WHERE CPId={}'''
MissVal = pd.read_sql(query.format(batchId),conn, parse_dates=['Date'])
print(MissVal)
# MissVal.drop_duplicates(subset=['Date'],inplace=True)
# Step 3 :- Generatte dates from min and max values
date_range = pd.DataFrame({'Date': pd.date_range(min(MissVal['Date']),max(MissVal['Date']), freq='D')})
# Step 4 :- Make left join with Missing values.
c = pd.merge(pd.DataFrame(date_range), pd.DataFrame(MissVal), 
              left_on=['Date'],
              right_on= ['Date'], how='left')
for col in c.columns:
    if not col=="Date":
      if not col=="Id":
        if not col=="CPId":
          c[col].interpolate(method ='linear',inplace=True)
for i, row in c.iterrows():
  if pd.isnull(row['CPId']):
        row['CPId']=batch
  else:
        row['CPId']=int(row['CPId'])
        batch=int(row['CPId'])
  SQLCommand = "INSERT INTO CentrifugalProcessedTables(CPId,Date,Vibration3H) VALUES('" + str(row['CPId']) + "','" + str(row['Date']) + "','" + str(row['Vibration3H']) + "')"
  cursor.execute(SQLCommand)     
conn.commit()


