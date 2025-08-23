import os
import json
import pandas as pd
from dotenv import load_dotenv
from geopy.geocoders import Nominatim

#Output Data with Columns
agg_ins_data={'state':[],'year':[], 'quarter':[], 'payment_category':[],'count':[],'amount':[]} #create empty dict
#Path to be specified to retrieve Data
data_agg_path1 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\aggregated\insurance\country\india\state"
#Converting to Dict
data_agg_list1 = os.listdir(data_agg_path1)
#print(data_agg_list1)

#Loop to Iterate to State,Years,Quarters
for state in data_agg_list1:
    years=os.listdir(f"{data_agg_path1}/{state}")
    for year in years:
        quarters=os.listdir(f"{data_agg_path1}/{state}/{year}")
        for qtr in quarters:
            path=f"{data_agg_path1}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)
#Loop to Iterate into JSON to get the required Columns and its Data
            for i in data['data']['transactionData']:
                Payment_Category=i['name']
                count=i['paymentInstruments'][0]['count'] #first one is dict, others are list
                amount=i['paymentInstruments'][0]['amount']
                agg_ins_data['state'].append(state)
                agg_ins_data['year'].append(int(year))
                agg_ins_data['quarter'].append(int(qtr.strip('.json')))
                agg_ins_data['payment_category'].append(Payment_Category)
                agg_ins_data['count'].append(count)
                agg_ins_data['amount'].append(amount)
#print(agg_ins_data)

#Converting this Data into DataFrame using Pandas
agg_ins_data_df=pd.DataFrame(agg_ins_data)

geolocator=Nominatim(user_agent="Agg_Ins_Map") #Nominatiom converts plcae name to lat and lon, # user agent is an Identifier

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in agg_ins_data_df['state'].unique():
    location=geolocator.geocode(i) #find lat, lon
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
agg_ins_data_df=pd.merge(agg_ins_data_df,state,on='state')

#To_CSV
agg_ins_data_df.to_csv("data/agg_insurance.csv", index=False)
#-----------------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
agg_trans_data={'state':[],'year':[], 'quarter':[], 'payment_category':[],'count':[],'amount':[]}

#Path to be specified to retrieve Data
data_agg_path2 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\aggregated\transaction\country\india\state"
#Converting to Dict
data_agg_list2 = os.listdir(data_agg_path2)
#print(data_agg_list)
#Loop to Iterate to State,Years,Quarters
for state in data_agg_list2:
    years=os.listdir(f"{data_agg_path2}/{state}")
    for year in years:
        quarters=os.listdir(f"{data_agg_path2}/{state}/{year}")
        for qtr in quarters:
            path=f"{data_agg_path2}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)
#Loop to Iterate into JSON to get the required Columns and its Data
            for i in data['data']['transactionData']:
                Payment_Category=i['name']
                count=i['paymentInstruments'][0]['count']
                amount=i['paymentInstruments'][0]['amount']
                agg_trans_data['state'].append(state)
                agg_trans_data['year'].append(int(year))
                agg_trans_data['quarter'].append(int(qtr.strip('.json')))
                agg_trans_data['payment_category'].append(Payment_Category)
                agg_trans_data['count'].append(count)
                agg_trans_data['amount'].append(amount)
#print(agg_ins_data)

#Converting this Data into DataFrame using Pandas
agg_trans_data_df=pd.DataFrame(agg_trans_data)

geolocator=Nominatim(user_agent="Agg_Trans_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in agg_trans_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
agg_trans_data_df=pd.merge(agg_trans_data_df,state,on='state')

#To_CSV
agg_trans_data_df.to_csv("data/agg_transactions.csv", index=False)
#------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
agg_user_data={'state':[],'year':[], 'quarter':[], 'registered_users':[],'brand':[],'count':[]}

#Path to be specified to retrieve Data
data_agg_path3 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\aggregated\user\country\india\state"
#Converting to Dict
data_agg_list3 = os.listdir(data_agg_path3)
#print(data_agg_list1)

#Loop to Iterate to State,Years,Quarters
for state in data_agg_list3:
    years=os.listdir(f"{data_agg_path3}/{state}")
    for year in years:
        quarters=os.listdir(f"{data_agg_path3}/{state}/{year}")
        for qtr in quarters:
            path=f"{data_agg_path3}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
            try:
                with open(path,'r') as f:
                    data=json.load(f)
                
#method2 
                registered_users=data['data']['aggregated']['registeredUsers']
                if data['data'].get('usersByDevice'):
                    device_list = data['data']['usersByDevice']
                else:
                    device_list = [{'brand': 'unknown', 'count': 0}] #to avoid key error

                for i in device_list:
                    agg_user_data['state'].append(state)
                    agg_user_data['year'].append(int(year))
                    agg_user_data['quarter'].append(int(qtr.strip('.json')))
                    agg_user_data['registered_users'].append(int(registered_users))
                    agg_user_data['brand'].append(i['brand'])
                    agg_user_data['count'].append(int(i['count']))

            except Exception as e:
                print(f"Skipped file: {path} due to error: {e}")

#Converting this Data into DataFrame using Pandas
agg_user_data_df=pd.DataFrame(agg_user_data)

geolocator=Nominatim(user_agent="Agg_User_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in agg_user_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
agg_user_data_df=pd.merge(agg_user_data_df,state,on='state')

#To_CSV
agg_user_data_df.to_csv("data/agg_users.csv", index=False)
#---------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
map_ins_data={'state':[],'year':[], 'quarter':[], 'latitude':[],'longitude':[],'metric':[],'label':[]}

#Path to be specified to retrieve Data
data_map_path1 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\map\insurance\country\india\state"
#Converting to Dict
data_map_list1 = os.listdir(data_map_path1)
#print(data_map_list1)

#Loop to Iterate to State,Years,Quarters
for state in data_map_list1:
    years=os.listdir(f"{data_map_path1}/{state}")
    for year in years:
        quarters=os.listdir(f"{data_map_path1}/{state}/{year}")
        for qtr in quarters:
            path=f"{data_map_path1}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)

                District_data=data['data']['data']['data']
                for i in District_data: #“In the JSON, each row is an array [lat, lon, metric, label]. 
                    latitude=i[0] #So in Python, we use i[0], i[1], i[2], i[3] to extract latitude, longitude, metric, and district name respectively.”
                    longitude=i[1]
                    metric=i[2]
                    label=i[3]
                    map_ins_data['state'].append(state)
                    map_ins_data['year'].append(int(year))
                    map_ins_data['quarter'].append(int(qtr.strip('.json')))
                    map_ins_data['latitude'].append(float(latitude))
                    map_ins_data['longitude'].append(float(longitude))
                    map_ins_data['metric'].append(float(metric))
                    map_ins_data['label'].append(label)
                    
                #print(map_ins_data)

#Converting this Data into DataFrame using Pandas
map_ins_data_df=pd.DataFrame(map_ins_data)

#To_CSV
map_ins_data_df.to_csv("data/map_insurance.csv", index=False)
#----------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
map_ins_hover_data={'state':[],'year':[], 'quarter':[], 'name':[],'count':[],'amount':[]}

#Path to be specified to retrieve Data
map_ins_hover_path1 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\map\insurance\hover\country\india\state"
#Converting to Dict
map_ins_hover_list1 = os.listdir(map_ins_hover_path1)
#print(map_ins_hover_list1)

#Loop to Iterate to State,Years,Quarters
for state in map_ins_hover_list1:
    years=os.listdir(f"{map_ins_hover_path1}/{state}")
    for year in years:
        quarters=os.listdir(f"{map_ins_hover_path1}/{state}/{year}")
        for qtr in quarters:
            path=f"{map_ins_hover_path1}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
        try:
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)

#Loop to Iterate into JSON to get the required Columns and its Data
                Data_list = data['data']['hoverDataList']

                for i in Data_list:
                      state_name = i['name']
                      count = i['metric'][0]['count']
                      amount = i['metric'][0]['amount']
                      map_ins_hover_data['state'].append(state)
                      map_ins_hover_data['year'].append(int(year))
                      map_ins_hover_data['quarter'].append(int(qtr.strip('.json')))
                      map_ins_hover_data['name'].append(state_name)
                      map_ins_hover_data['count'].append(count)
                      map_ins_hover_data['amount'].append(amount)
            #print(map_ins_hover_data)  
        except Exception as e:
            print(f"Skipped file:{path} due to error {e}")

#Converting this Data into DataFrame using Pandas
map_ins_hover_data_df = pd.DataFrame(map_ins_hover_data)

geolocator=Nominatim(user_agent="Map_Ins_Hover_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in map_ins_hover_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
map_ins_hover_data_df=pd.merge(map_ins_hover_data_df,state,on='state')

#To_CSV
map_ins_hover_data_df.to_csv("data/maphover_insurance.csv", index=False)
#print(map_ins_hover_data_df)
#------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
map_trans_data={'state':[],'year':[], 'quarter':[], 'name':[],'count':[],'amount':[]}

#Path to be specified to retrieve Data
map_trans_path2 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\map\transaction\hover\country\india\state"
#Converting to Dict
map_trans_list2 = os.listdir(map_trans_path2)
#print(map_trans_list2)

#Loop to Iterate to State,Years,Quarters
for state in map_trans_list2:
    years=os.listdir(f"{map_trans_path2}/{state}")
    for year in years:
        quarters=os.listdir(f"{map_trans_path2}/{state}/{year}")
        for qtr in quarters:
            path=f"{map_trans_path2}/{state}/{year}/{qtr}"
            #print(path)
#Loading the JSON FILE
        try:
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)

                District_data=data['data']['hoverDataList']
                for i in District_data:
                    name=i['name']
                    count=i['metric'][0]['count']
                    amount=i['metric'][0]['amount']
                    map_trans_data['state'].append(state)
                    map_trans_data['year'].append(int(year))
                    map_trans_data['quarter'].append(int(qtr.strip('.json')))
                    map_trans_data['name'].append(name)
                    map_trans_data['count'].append(int(count))
                    map_trans_data['amount'].append(int(amount))

        except Exception as e:
            print(f"Skipped file: {path} due to error: {e}")

#Converting this Data into DataFrame using Pandas
map_trans_data_df=pd.DataFrame(map_trans_data)

geolocator=Nominatim(user_agent="Map_Trans_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in map_trans_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
map_trans_data_df=pd.merge(map_trans_data_df,state,on='state')
#To_CSV
map_trans_data_df.to_csv("data/map_transaction.csv", index=False)
#print(map_trans_data_df)
#----------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
map_user_data={'state':[],'year':[], 'quarter':[], 'district_name':[],'registered_users':[],'app_opens':[]}

#Path to be specified to retrieve Data
map_user_path3 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\map\user\hover\country\india\state"

#Converting to Dict
map_user_list3 = os.listdir(map_user_path3)
#print(map_user_list3)

#Loop to Iterate to State,Years,Quarters
for state in map_user_list3:
    years=os.listdir(f"{map_user_path3}/{state}")
    for year in years:
        quarters=os.listdir(f"{map_user_path3}/{state}/{year}")
        for qtr in quarters:
            path=f"{map_user_path3}/{state}/{year}/{qtr}"
            #print(path)

#Loading the JSON FILE
        try:
            with open(path,'r') as f:
                data=json.load(f)
                #print(data)

                district_data=data['data']['hoverData']
                for district,data in district_data.items(): #imp
                    reg_users=data.get('registeredUsers',0)
                    app_opens=data.get('appOpens',0)
                    map_user_data['state'].append(state)
                    map_user_data['year'].append(int(year))
                    map_user_data['quarter'].append(int(qtr.strip('.json')))
                    map_user_data['district_name'].append(district)
                    map_user_data['registered_users'].append(int(reg_users))
                    map_user_data['app_opens'].append(int(app_opens))

        except Exception as e:
            print(f"Skipped file: {path} due to error: {e}")

#Converting this Data into DataFrame using Pandas
map_user_data_df=pd.DataFrame(map_user_data)

geolocator=Nominatim(user_agent="Map_User_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in map_user_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
map_user_data_df=pd.merge(map_user_data_df,state,on='state')
#To_CSV
map_user_data_df.to_csv("data/map_user.csv", index=False)
#print(map_user_data_df)
#---------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
top_ins_data={'state':[],'year':[], 'quarter':[], 'state_entity':[],'s_count':[],'s_amount':[],'district_entity':[],'d_count':[],'d_amount':[],'pincode':[],'p_count':[],'p_amount':[]}

#Path to be specified to retrieve Data
top_ins_path1 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\top\insurance\country\india\state"
#Converting to Dict
top_ins_list1 = os.listdir(top_ins_path1)
#print(top_ins_list1)

#Loop to Iterate to State,Years,Quarters
for state in top_ins_list1:
    years=os.listdir(f"{top_ins_path1}/{state}")
    for year in years:
        quarters=os.listdir(f"{top_ins_path1}/{state}/{year}")
        for qtr in quarters:
            path=f"{top_ins_path1}/{state}/{year}/{qtr}"
            #print(path)
            
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    #It fills missing values, prevents from crashing or empty DF by filling default values automatically
                    state_name = data['data'].get('states') or [{'entityName': 'NA', 'metric': {}}]
                    district_name = data['data'].get('districts') or [{'entityName':'NA','metric':{}}]
                    pincode_name = data['data'].get('pincodes') or [{'entityName':'NA','metric':{}}]
            
                    for i in state_name:
                        state_entity=i.get('entityName','NA')
                        s_metrics=i.get('metric',{})
                        s_count=s_metrics.get('count',0)
                        s_amount=s_metrics.get('amount',0)
                        for j in district_name:
                            district_entity = j.get('entityName', 'NA')
                            d_metrics=j.get('metric',{})
                            d_count=d_metrics.get('count',0)
                            d_amount=d_metrics.get('amount',0)
                            for k in pincode_name:
                                pincode = k.get('entityName','NA')
                                p_metrics=k.get('metric',{})
                                p_count = p_metrics.get('count',0)
                                p_amount = p_metrics.get('amount',0)

                                top_ins_data['state'].append(state)
                                top_ins_data['year'].append(int(year))
                                top_ins_data['quarter'].append(int(qtr.strip('.json')))
                                top_ins_data['state_entity'].append(state_entity)
                                top_ins_data['s_count'].append(int(s_count))
                                top_ins_data['s_amount'].append(float(s_amount))
                                top_ins_data['district_entity'].append(district_entity)
                                top_ins_data['d_count'].append(int(d_count))
                                top_ins_data['d_amount'].append(float(d_amount))
                                top_ins_data['pincode'].append(str(pincode))
                                top_ins_data['p_count'].append(int(p_count))
                                top_ins_data['p_amount'].append(float(p_amount))
                 
            except Exception as e:
                print(f"Skipped file: {path} due to error: {e}")

#Converting this Data into DataFrame using Pandas
top_ins_data_df = pd.DataFrame(top_ins_data)

geolocator=Nominatim(user_agent="Top_Ins_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in top_ins_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
top_ins_data_df=pd.merge(top_ins_data_df,state,on='state')
#To_CSV
top_ins_data_df.to_csv("data/top_insurance.csv",index=False)
#print(top_ins_data_df)
#------------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
top_trans_data={'state':[],'year':[], 'quarter':[],'state_name':[],'s_count':[],'s_amount':[],'district_name':[],'d_count':[],'d_amount':[],'pincode':[],'p_count':[],'p_amount':[]}

#Path to be specified to retrieve Data
top_trans_path2 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\top\transaction\country\india\state"
#Converting to Dict
top_trans_list2 = os.listdir(top_trans_path2)
#print(top_ins_list1)

#Loop to Iterate to State,Years,Quarters
for state in top_trans_list2:
    years=os.listdir(f"{top_trans_path2}/{state}")
    for year in years:
        quarters=os.listdir(f"{top_trans_path2}/{state}/{year}")
        for qtr in quarters:
            path=f"{top_trans_path2}/{state}/{year}/{qtr}"
            #print(path)
            
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    #print(data)
         #It fills missing values, prevents from crashing or empty DF by filling default values automatically
                    State_data=data['data'].get('states') or [{'entityName': 'NA', 'metric': {}}] 
                    District_data=data['data'].get('districts') or [{'entityName': 'NA', 'metric': {}}]
                    Pincode_data=data['data'].get('pincodes') or [{'entityName': 'NA', 'metric': {}}]
                    for i in State_data:
                        State_name=i.get('entityName','NA')
                        s_metrics=i.get('metric',{})
                        s_count=s_metrics.get('count',0)
                        s_amount=s_metrics.get('amount',0)
                        
                        for j in District_data:
                            District_name = j.get('entityName', 'NA')
                            d_metrics=j.get('metric',{})
                            d_count=d_metrics.get('count',0)
                            d_amount=d_metrics.get('amount',0)

                            for k in Pincode_data:
                                Pincode_name = k.get('entityName','NA')
                                p_metrics=k.get('metric',{})
                                p_count = p_metrics.get('count',0)
                                p_amount = p_metrics.get('amount',0)
                                
                                top_trans_data['state'].append(state)
                                top_trans_data['year'].append(int(year))
                                top_trans_data['quarter'].append(int(qtr.strip('.json')))
                                top_trans_data['state_name'].append(State_name)
                                top_trans_data['s_count'].append(int(s_count))
                                top_trans_data['s_amount'].append(float(s_amount))
                                top_trans_data['district_name'].append(District_name)
                                top_trans_data['d_count'].append(int(d_count))
                                top_trans_data['d_amount'].append(float(d_amount))
                                top_trans_data['pincode'].append(str(Pincode_name))
                                top_trans_data['p_count'].append(int(p_count))
                                top_trans_data['p_amount'].append(float(p_amount))

            except Exception as e:
                print(f"Skipped file: {path}' due to error {e}")

#Converting this Data into DataFrame using Pandas
top_trans_data_df=pd.DataFrame(top_trans_data)
geolocator=Nominatim(user_agent="Top_Trans_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in top_trans_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
top_trans_data_df=pd.merge(top_trans_data_df,state,on='state')
#To_CSV
top_trans_data_df.to_csv("data/top_transaction.csv",index=False)
#print(top_trans_data_df)     
#----------------------------------------------------------------------------------------------------------------------------------
#Output Data with Columns
top_user_data={'state':[],'year':[], 'quarter':[],'state_name':[],'s_regusers':[],'district_name':[],'d_regusers':[],'pincode':[],'p_regusers':[]}

#Path to be specified to retrieve Data
top_trans_path3 = r"C:\Users\sripe\OneDrive\Desktop\Phone_Pay\pulse\data\top\user\country\india\state"
#Converting to Dict
top_user_list3 = os.listdir(top_trans_path3)
#print(top_ins_list3)

#Loop to Iterate to State,Years,Quarters
for state in top_user_list3:
    years=os.listdir(f"{top_trans_path3}/{state}")
    for year in years:
        quarters=os.listdir(f"{top_trans_path3}/{state}/{year}")
        for qtr in quarters:
            path=f"{top_trans_path3}/{state}/{year}/{qtr}"
            #print(path)

            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    #print(data)

                    State_data=data['data'].get('states') or [{'name': 'NA', 'registeredUsers': 0}]
                    District_data=data['data'].get('districts') or [{'name': 'NA', 'registeredUsers': 0}]
                    Pincode_data=data['data'].get('pincodes') or [{'name': 'NA', 'registeredUsers': 0}]
                    
                    for i in State_data:
                        State_name=i.get('name', 'NA')
                        s_regusers=i.get('registeredUsers', 0)
                        for j in District_data:
                            District_name=j.get('name','NA')
                            d_regusers=j.get('registeredUsers',0)
                            for k in Pincode_data:
                                Pincode_name=k.get('name','NA')
                                p_regusers=k.get('registeredUsers',0)
                                
                                top_user_data['state'].append(state)
                                top_user_data['year'].append(int(year))
                                top_user_data['quarter'].append(int(qtr.strip('.json')))
                                top_user_data['state_name'].append((State_name))
                                top_user_data['s_regusers'].append(int(s_regusers))
                                top_user_data['district_name'].append((District_name))
                                top_user_data['d_regusers'].append(int(d_regusers))
                                top_user_data['pincode'].append(str(Pincode_name))
                                top_user_data['p_regusers'].append(int(p_regusers))

            except Exception as e:
                print(f"Skipped file: {path}' due to error {e}")

#Converting this Data into DataFrame using Pandas
top_user_data_df=pd.DataFrame(top_user_data)
geolocator=Nominatim(user_agent="Top_User_Map")

state_lat_long = {'state': [], 'lat': [], 'lon': []}
for i in top_user_data_df['state'].unique():
    location=geolocator.geocode(i)
    lat=location.latitude
    lon=location.longitude
    state_lat_long['state'].append(i)
    state_lat_long['lat'].append(lat)
    state_lat_long['lon'].append(lon)
    #print(i,lat,lon)

state=pd.DataFrame(state_lat_long)
top_user_data_df=pd.merge(top_user_data_df,state,on='state')
#To_CSV
top_user_data_df.to_csv("data/top_user.csv", index=False)
#print(top_user_data_df)
#-----------------------------------------------------------------------------------------------------------------------------------
#pip install sqlAlchemy
#pip install psycopg2

from sqlalchemy import create_engine
import psycopg2

load_dotenv()

# Get credentials from .env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

#Establishing the connection between Py and SQL
conn = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)

#cursor is used to execute commands in SQL
cur = conn.cursor()
conn.autocommit=True

#Creating new DB 
new_db_name="phone_pe"
cur.execute(f"CREATE DATABASE {new_db_name}")

#Connecting New DB
conn=psycopg2.connect(
    database="phone_pe",
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)
cur = conn.cursor()

#Creating new table - Agg Insurance
create_table='''
CREATE TABLE IF NOT EXISTS agg_insurance_data(
    id serial primary key,
    state varchar(250),
    year integer,
    quarter varchar(50),
    payment_category varchar(250),
    count Integer,
    amount Float,
    lat float,
    lon float
);'''

cur.execute(create_table)
conn.commit()

#Creating new table - Agg Transaction
create_table='''
CREATE TABLE IF NOT EXISTS agg_transaction_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    payment_category varchar(1000),
    count BIGINT,
    amount Float,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Agg User
create_table='''
CREATE TABLE IF NOT EXISTS agg_user_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    registered_users bigint,
    brand varchar(250),
    count BIGINT,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Map Insurance
create_table='''
CREATE TABLE IF NOT EXISTS map_ins_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    latitude float,
    longitude float,
    metric float,
    label TEXT
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Map Insurance Hover
create_table='''
CREATE TABLE IF NOT EXISTS map_ins_hover_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    name varchar(255),
    count BIGINT,
    amount float,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Map Transaction
create_table='''
CREATE TABLE IF NOT EXISTS map_trans_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    name varchar(255),
    count BIGINT,
    amount BIGINT,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Map User
create_table='''
CREATE TABLE IF NOT EXISTS map_user_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    district_name varchar(255),
    registered_users BIGINT,
    app_opens BIGINT,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Top Ins
create_table='''
CREATE TABLE IF NOT EXISTS top_ins_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    state_entity varchar(255),
    s_count integer,
    s_amount float,
    district_entity varchar(255),
    d_count integer,
    d_amount float,
    pincode varchar(255),
    p_count integer,
    p_amount float,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Top Trans
create_table='''
CREATE TABLE IF NOT EXISTS top_trans_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    state_name varchar(1000),
    s_count integer,
    s_amount float,
    district_name varchar(255),
    d_count integer,
    d_amount float,
    pincode varchar(255),
    p_count BIGINT,
    p_amount float,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Creating new table - Top User
create_table='''
CREATE TABLE IF NOT EXISTS top_user_data(
    id serial primary key,
    state varchar(1000),
    year integer,
    quarter varchar(1000),
    state_name varchar(255),
    s_regusers integer,
    district_name varchar(255),
    d_regusers integer,
    pincode varchar(255),
    p_regusers integer,
    lat float,
    lon float
);'''
cur.execute(create_table)
conn.commit()

#Create SQLAlchemy engine to connect Dataframe with SQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
agg_ins_data_df.to_sql(name="agg_insurance_data",con=engine,if_exists="append",index=False)
agg_trans_data_df.to_sql(name="agg_transaction_data",con=engine,if_exists="append",index=False)
agg_user_data_df.to_sql(name="agg_user_data",con=engine,if_exists="append",index=False)
map_ins_data_df.to_sql(name="map_ins_data",con=engine,if_exists="append",index=False)
map_ins_hover_data_df.to_sql(name="map_ins_hover_data",con=engine,if_exists="append",index=False)
map_trans_data_df.to_sql(name="map_trans_data",con=engine,if_exists="append",index=False)
map_user_data_df.to_sql(name="map_user_data",con=engine,if_exists="append",index=False)
top_ins_data_df.to_sql(name="top_ins_data",con=engine,if_exists="append",index=False)
top_trans_data_df.to_sql(name="top_trans_data",con=engine,if_exists="append",index=False)
top_user_data_df.to_sql(name="top_user_data",con=engine,if_exists="append",index=False)










                       
