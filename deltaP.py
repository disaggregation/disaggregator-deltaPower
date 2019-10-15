
# coding: utf-8

# In[1]:

import sqlite3
# import os.path
import hashlib
import pandas as pd
import numpy as np


db = './../data/daf06fb3ab69f27bd681a63311722181.db'
#db = './test.db'
db = '../data/disaggregation.db'

conn = sqlite3.connect(db)
#c = conn.cursor()
#c.execute("DELETE FROM devices WHERE device_id < 277;")
df = pd.read_sql_query("SELECT date as 'datetime', demand_Power-supply_Power as 'demand' FROM loads ORDER BY date desc LIMIT 3600;", conn)
conn.close()# close db
    
  
df['datetime'] = pd.to_datetime(df['datetime'])
df.sort_values(by='datetime',ascending=True);


# In[2]:

df['dp'] = df['demand'] - df['demand'].shift(1)
df['dp+'] = [0-x if x <=0 else 0 for x in df['dp']]
df['dp-'] = [x if x >=0 else 0 for x in df['dp']]

df['dp'] = df['demand'] - df['demand'].shift(1)
for x in range(1,len(df)):
    df.loc[x,'dp'] = df.loc[x,'demand'] -  df.loc[x-1,'demand']
    if df.loc[x,'dp'] > 0:
        df.loc[x,'dp+'] = df.loc[x,'dp']
        df.loc[x,'dp-'] = 0
    else:
        df.loc[x,'dp-'] = 0-df.loc[x,'dp']
        df.loc[x,'dp+'] = 0


# In[3]:

# add user defined loads
user_loads = {'modem':10,'rPi':4}
baseload = 0
for user_load in user_loads:
    df.loc[:,user_load]  = user_loads[user_load]
    baseload = baseload + user_loads[user_load]
#     df.loc[:,] = np.array([df['demand'].min()] * len(df))


# In[4]:

# find baseload and determine remaining unkwown profile
df.loc[:,'baseload'] = np.array([df['demand'].min()-baseload] * len(df))
df.loc[:,'unknown'] = df.loc[:,'demand'] - df.loc[:,'baseload']-baseload


# In[5]:

# df[pd.isnull(df).any(axis=1)]


# In[6]:

def left_round(number,precision=2):
    rem = number

    d = len(str(number))
    while(round(rem,d) == round(number,d)):
        rem = round(number,d-precision)
        d = d -1
        
    i = 0
    while(rem > 10**precision):
        rem = round(rem / 10,0)
        i = i +1
#         print(rem*(10**i))
        if i > 100:
            break
    return rem*(10**i)

left_round(00.00034564,3)

left_round(122431234.00034564,3)

# left_round(123456)


# In[7]:

#find square loads
loads = dict()

for x in range(1,len(df)):
    #filter for dP+ with minimim power
    if df.loc[x,'dp+'] > 100:
        print(' ') #new line in output
        print ('start dp>100:',df.loc[x,'datetime'],'start P:',df.loc[x-1,'unknown'],'dP+:' ,df.loc[x,'dp+'])
        
        #look forwar in time to see how long load remains on
        for y in range(x+1,len(df)):
            #find dP- of same/simulair size 
            if df.loc[y,'dp-'] > 0.9 *df.loc[x,'dp+'] and df.loc[y,'dp-'] < 1.1 *df.loc[x,'dp+']:
                print ('stop dP-:',df.loc[y,'datetime'],'dP-:' ,df.loc[y,'dp-'])
                break
            # check if does not drop below start power (this means is must have been turned of)    
            if df.loc[y,'unknown'] <= df.loc[x-1,'unknown']:
                print ('stop demand:',df.loc[x,'datetime'] ,df.loc[y,'unknown'], ' <= ',df.loc[x-1,'unknown'])
                break
        
        print(x,y,(df.loc[x:y-1,'unknown']-df.loc[x-1,'unknown']).mean())
        
        # calculate key parameters
        mean = (df.loc[x:y-1,'unknown']-df.loc[x-1,'unknown']).mean()
        max = (df.loc[x:y-1,'unknown']-df.loc[x-1,'unknown']).max()
        min = (df.loc[x:y-1,'unknown']-df.loc[x-1,'unknown']).min()
        std = (df.loc[x:y-1,'unknown']-df.loc[x-1,'unknown']).std()
        time = pd.Timedelta(pd.to_datetime(df['datetime'][y])-pd.to_datetime(df['datetime'][x]))
        noise = df.loc[x:y-1,'dp'].abs().mean()
        
        print('mean load:',mean)
        print('max load:',max)
        print('std load:',std)
        print('noise:',noise)
        print('time past:',time)


        print('found profile from ',df.loc[x-1,'datetime'],'to',df.loc[y,'datetime'])
        profile = list(df.loc[x-1:y,'unknown']-df.loc[x-1,'unknown'])
        print(profile)
        start = str(df.loc[x-1,'datetime'])
        stop = str(df.loc[y,'datetime'])
#         plt.plot(range(x,y),profile)
#         plt.title('A simple chirp')
#         plt.show()
#         break
        try:
            #drop loads which do not have a square looking profile.
            if (std/mean > 0.1):
                print('dropped because of large std:',std/mean)
                continue

            #drop loads which do not have odd start compared to mean
            if( abs(mean-df.loc[x,'dp+'])/df.loc[x,'dp+'] > 0.2):    
                print('dropped because large difference between mean and start dp+:',abs(mean-df.loc[x,'dp+'])/df.loc[x,'dp+'])
                continue
        except:
            print('Something went wrong in analysing found load, dropping ....')
            continue
#         print('hist:',df.hist(bins=10))
        
        # save this load to the distionairy (group by 'fingerprint')
        fingerprint = str(int(round(mean,-2)))+'_'#+str(int(left_round(df.loc[x,'dp+'],2)))
        temp = {'index':{'start':x,'stop':y},'time':time,'mean':mean,'max':max,'profile':profile}
        try:
            loads[fingerprint][len(loads[fingerprint])] = temp
        except:
            loads[fingerprint] = {0:temp}
            df[fingerprint] = 0 #add new colum to the dataframe before inserting found loads (only on first occurance!)
        
        df.loc[x:y-1,fingerprint] = profile[1:-1]
        df.loc[x:y-1,'unknown'] = df.loc[x:y-1,'unknown'] - profile[1:-1]
        print('Saved with fingerprint:',fingerprint)
        try:
		print("INSERT INTO `devices` (author_id,name,power,first_datetime,last_datetime) VALUES (0,'"+fingerprint+"','"+str(mean)+"','"+start+"','"+stop+"');")
        	conn = sqlite3.connect(db)
        	c = conn.cursor()

	        sql = '''CREATE TABLE loads (date datetime, lowtarif_demand real, hightarif_demand real, 
                                    lowtarif_supply real, hightarif_supply real, demand_power real, 
                                    supply_power real, gas_demand real, demand_power_L1 real, 
                                    demand_power_L2 real, demand_power_L3 real, supply_power_L1 real,
                                    supply_power_L2 real, supply_power_L3 real, voltage real, current real)'''
        	c.execute("INSERT INTO `devices` (author_id,name,power,first_datetime,last_datetime) VALUES (0,'"+fingerprint+"','"+str(mean)+"','"+start+"','"+stop+"');")
        	conn.commit()
        	conn.close() #close db
        	print('saved')
	except:
		print('failed')


# In[8]:

import plotly.offline
import plotly.graph_objs as go
import cufflinks
cufflinks.go_offline()


# In[9]:

start = 0
points = 1000
try:
    start = plots * points
except:
    plots = 0
# plots = 0

    
res = df.set_index('datetime')
res[start:start+points].iplot(kind='line')

plots = plots +1
# df[].iplot()


# In[10]:

# print(list(loads['100_'][0]))
for i in sorted(loads):
    print(i,'has',len(loads[i]),'occurance(s)')
#     for load in loads[i]:
#         if i == '100_':
#             print(round(sum(loads[i][load]['profile'])/360,1),'Wh in ',loads[i][load]['time'])
            


# In[ ]:




# In[11]:


res = df.set_index('datetime')
# res[:1000].iplot(kind='line')
# fig = res.plot()
# fig.show()
res.resample('10T').iplot(kind='line')



# res[:100]


# In[12]:


# df[['datetime',list(loads)]].iplot()
temp = list(loads)
temp[0] = 'datetime'
temp[1] = 'baseload'
temp[2] = 'unknown'

temp[3:] =list(loads)
dp = df[temp]
dp = dp.set_index('datetime')
dp.resample('10T').mean().iplot(kind='line')
# dp.resample('10T').mean().iplot(kind='area',fill='tozeroy',mode='none')



# In[ ]:




# In[13]:

pie = dp.sum()
# values = pie.values/pie.sum()*100
round(pie/pie.sum()*100,1)


# In[14]:

trace = go.Pie(labels=pie.index, values=pie.values)
dp.iplot([trace], filename='disaggregator_results_pie')


# In[15]:

# import plotly.plotly as py
import plotly.offline as py

trace = go.Pie(labels=pie.index, values=pie.values)

py.plot([trace], filename='disaggregator_results_pie.html')


# # Try to rename colums for 'kwown loads' 

# In[16]:

#relable output for known_loads
known_loads = {'100_':'Fridge','2000_':'Iron'}
for name in loads.keys():
    try:
        df=df.rename(columns = {name:known_loads[name]})
        loads[known_loads[name]] = loads[name]
    except:
        print(name,'not found known_loads')
    
for name in known_loads:
    try:
        del loads[name]
    except:
        print(name,'not found in legend')

legend = loads.keys()
print(list(legend))

