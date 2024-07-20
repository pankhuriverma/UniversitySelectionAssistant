from random import lognormvariate
from types import LambdaType
from flask import Flask,json,request
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import pandas as pd
import requests
import time
from datetime import datetime, timezone
from opencage.geocoder import OpenCageGeocode

app = Flask(__name__)
databaseTask1 = 'info_integration_task1' 
databaseTask2 = 'info_integration_task2'
databaseTask4 = 'II_T4' 
username = 'root' 
password = 'mysql'
weather_api_key = 'c1ae9097e6f089ad74f17f63fbd18b9d'
base_url = "http://api.openweathermap.org/data/2.5/weather?"
p1 = "6e2d41e5e07144ca8cc6cd76e8891c8a"
p2 = "7198b251faa44fc780badb7b62d62162"

CORS(app)
@app.route('/home',methods=['GET'])
def getUniData():
    # extract_load_uni_Data()
    json_data=[]
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM uni_data where ranking <> 0 order by ranking')
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()
    return json.dumps(json_data)

@app.route('/getPollutionData',methods=['GET'])
def getUniDataPollution():
    selectedRegion = request.args.get('selectedRegion')
    selectedValueofRegion = request.args.get('selectedVal').lower()
    json_data=[]
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        if(selectedValueofRegion != 'undefined'):
            query = "select u.ranking,u.institution,l.city,l.state,l.country,t.aqi,t.co,t.no2,t.nh3,t.so2,t.timezone,t.currency, \
               t.drive_on,t.speed_in from uni_data u,location l,topology t where lower(l."+selectedRegion+")=\""+selectedValueofRegion+"\" AND u.uni_key=l.uni_key and \
            l.city=t.city and l.state=t.state and l.country=t.country and ranking>0 and t.aqi<=3 order by u.ranking"
        else:
            query = "select u.ranking,u.institution,l.city,l.state,l.country,t.aqi,t.co,t.no2,t.nh3,t.so2,t.timezone,t.currency, \
               t.drive_on,t.speed_in from uni_data u,location l,topology t where u.uni_key=l.uni_key and \
            l.city=t.city and l.state=t.state and l.country=t.country and ranking>0 and t.aqi<=3 order by u.ranking"
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()
    return json.dumps(json_data)

@app.route('/getWeatherData',methods=['GET'])
def getUniDataWeather():
    selectedRegion = request.args.get('selectedRegion')
    selectedValueofRegion = request.args.get('selectedVal').lower()
    min_temp = request.args.get('min_temp')
    max_temp = request.args.get('max_temp')
    json_data=[]
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        if(selectedValueofRegion != 'undefined'):
            if(min_temp == 'null' and max_temp == 'null'):
                query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity \
                from location l,uni_data u, weather w where lower(l."+selectedRegion+")=\""+selectedValueofRegion+"\" AND l.uni_key=u.uni_key and w.city=l.city\
                and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
            elif(min_temp != 'null' and max_temp == 'null'):
                query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,\
                    w.humidity from location l,uni_data u, weather w where lower(l."+selectedRegion+")=\""+selectedValueofRegion+"\" AND \
                    w.min_temp>="+min_temp+" and l.uni_key=u.uni_key \
                    and w.city=l.city and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
            elif(min_temp == 'null' and max_temp != 'null'):
                query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity \
                    from location l,uni_data u, weather w where lower(l."+selectedRegion+")=\""+selectedValueofRegion+"\" AND \
                        w.max_temp<="+max_temp+" and \
                        l.uni_key=u.uni_key and w.city=l.city and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
            elif(min_temp != 'null' and max_temp != 'null'):
                query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity \
                    from location l,uni_data u, weather w where lower(l."+selectedRegion+")=\""+selectedValueofRegion+"\" AND \
                        w.min_temp>="+min_temp+" and w.max_temp<="+max_temp+" and l.uni_key=u.uni_key and w.city=l.city \
                            and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
        elif(min_temp == 'null' and max_temp == 'null'):
            query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity\
                 from location l,uni_data u, weather w where l.uni_key=u.uni_key and w.city=l.city\
                      and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
        elif(min_temp != 'null' and max_temp == 'null'):
            query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity from location l,uni_data u, weather w where w.min_temp>="+min_temp+" and l.uni_key=u.uni_key and w.city=l.city and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
        elif(min_temp == 'null' and max_temp != 'null'):
            query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity from location l,uni_data u, weather w where w.max_temp<="+max_temp+" and l.uni_key=u.uni_key and w.city=l.city and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
        elif(min_temp != 'null' and max_temp != 'null'):
            query = "select u.ranking,u.institution,l.city,l.state,l.country,w.min_temp,w.max_temp,w.wind,w.pressure,w.humidity from location l,uni_data u, weather w where w.min_temp>="+min_temp+" and w.max_temp<="+max_temp+" and l.uni_key=u.uni_key and w.city=l.city and w.state=l.state and w.country=l.country and u.ranking>0 order by u.ranking"
        print(query)
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()
    return json.dumps(json_data)

@app.route('/region',methods=['GET'])
def getWeatherData():
    # extract_load_uni_Data()
    selectedRegion = request.args.get('selectedRegion')
    json_data=[]
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute('SELECT distinct('+selectedRegion+') FROM weather')
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()
    return json.dumps(json_data)

def extract_load_uni_Data(filename):
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask2,user=username,password=password)
        
        if conn.is_connected():
            cursor = conn.cursor()
            if(filename == "universities_ranking.csv"):
                uniData = pd.read_csv(filename, delimiter = ',',error_bad_lines=False)
                uniData.head()
                for i,row in uniData.iterrows():
                    # row[0] = int(row[0])
                    row[6] = str(row[6])
                    if row[6]=='nan': row[6] = ""
                    # row[4] = row[4].replace("\"","")
                    row[3] = row[3].replace(",","")
                    try:
                        row[5] = row[5].replace("%","")
                        # if(row[6] != ""):
                        #     row[6] = float(row[6])
                    except:
                        print(row)

                    query = "insert into uni_rank_2021(ranking,title,location,num_st,stu_stf_rt,int_st,gender_rt) values(%s,%s,%s,%s,%s,%s,%s)"
                    try:
                        cursor.execute(query,tuple(row))
                    except Error as e:
                        print("Record Insertion failed for: "+ str(row[0]) + " with error: "+ str(e))
                    conn.commit()
            elif(filename == "2020-QS-World-University-Rankings.csv"):
                uniData = pd.read_csv(filename, delimiter = ',',error_bad_lines=False)
                uniData.head()
                for i,row in uniData.iterrows():
                    row = [str(sub).replace('+', '') for sub in row]
                    # row = [str(sub).replace(' ', '') for sub in row]
                    row = [str(sub).replace('=', '') for sub in row]
                    if row[0]=='nan' : row[0] = "0"
                    if row[1]=='nan' : row[1] = "0"
                    if row[9]=='nan' or row[9]=='-': row[9] = "0"
                    if row[10]=='nan' or row[10]=='-': row[10] = "0"
                    if row[11]=='nan' or row[11]=='-': row[11] = "0"
                    if row[12]=='nan' or row[12]=='-': row[12] = "0"
                    if row[13]=='nan' or row[13]=='-': row[13] = "0"
                    if row[14]=='nan' or row[14]=='-': row[14] = "0"
                    if row[15]=='nan' or row[15]=='-': row[15] = "0"
                    if row[16]=='nan' or row[16]=='-': row[16] = "0"
                    if row[17]=='nan' or row[17]=='-': row[17] = "0"
                    if row[18]=='nan' or row[18]=='-': row[18] = "0"
                    if row[19]=='nan' or row[19]=='-': row[19] = "0"
                    if row[20]=='nan' or row[20]=='-': row[20] = "0"
                    if row[21]=='nan' or row[21]=='-': row[21] = "0"
                    if('-' in row[21] and len(row[21]) > 2):
                        temp_row = row[21].split('-')
                        row[21] = (float(temp_row[0]) + float(temp_row[1])) / 2
                    if('-' in row[0] and len(row[0]) > 2):
                        temp_row = row[0].split('-')
                        row[0] = (float(temp_row[0]) + float(temp_row[1])) / 2
                    if('-' in row[1] and len(row[1]) > 2):
                        temp_row = row[1].split('-')
                        row[1] = (float(temp_row[0]) + float(temp_row[1])) / 2
                    temp_row = [row[0],row[1],row[2],row[3],row[4],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],float(row[17]),float(row[18]),row[19],row[20],row[21]]
                    query = "insert into uni_qs_rank_19_20(rank_in_2020 ,rank_in_2019, institution_name, country, size, status,academic_rep_sc, academic_rep_rk , emp_rep_sc ,emp_rep_rk,faculty_stu_sc ,faculty_stu_rk,citation_fac_sc,citation_fac_rk,intl_fac_sc,intl_fac_rk,intl_stu_sc,intl_stu_rk,overall_score) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    try:
                        cursor.execute(query,tuple(temp_row))
                    except Error as e:
                        print("Record Insertion failed for: "+ str(temp_row) + "ROW 18" + str(row[18]) + " with error: "+ str(e))
                    conn.commit()
            else:
                uniData = pd.read_excel(filename)
                uniData.head()
                # uniData.to_sql(name='uni_world_rank_19_20', con=conn)
                for i,row in uniData.iterrows():
                    row[8] = float(row[8])
                    temp_row = [row[0],row[1],row[2],row[8]]
                    query = "insert into uni_world_rank_19_20(world_rank,institution,location,score) values(%s,%s,%s,%s)"
                    try:
                        cursor.execute(query,tuple(temp_row))
                    except Error as e:
                        print("Record Insertion failed for: "+ str(row[0]) + " with error: "+ str(e))
                conn.commit()
        conn.close()
    except Error as e:
        print(str(e))

def getLocationData():
    json_data=[]
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute('select uni_key,latitude,longitude,city,state,country from location group by city,state,country order by state;')
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    return json.dumps(json_data)

def weather_info():
    city_data = json.loads(getLocationData())
    print("Number of locations in the data source: " + str(len(city_data)))

    if (len(city_data) > 0):
        batchsize = 50
        for j in range(0,len(city_data),batchsize):
            batch = city_data[j:j+batchsize]
            for i in batch:
                complete_url = base_url+"lat="+str(i['latitude'])+"&lon="+str(i['longitude'])+"&units=metric&appid="+weather_api_key
                print(complete_url)
                # api response
                response = requests.get(complete_url)
                json_response = response.json()

                if(json_response["cod"]!=404):
                    weather = parse_city_weather(json_response,i['city'],i['state'],i['country'])
                    db_output = insert_weather_data(weather)
            
            time.sleep(60)

def parse_city_weather(weather_json,city,state,country):
    weather = []
    try:
        weather.append(city)
        weather.append(state)
        weather.append(country)
        weather.append(weather_json["main"]["temp"])
        weather.append(weather_json["main"]["temp_max"])
        weather.append(weather_json["main"]["temp_min"])
        weather.append(weather_json["main"]["feels_like"])
        weather.append(weather_json["main"]["pressure"])
        weather.append(weather_json["main"]["humidity"])
        weather.append(weather_json["wind"]["speed"])
        dt_object = datetime.fromtimestamp(weather_json["sys"]["sunrise"])
        weather.append(dt_object)
        dt_object = datetime.fromtimestamp(weather_json["sys"]["sunset"])
        weather.append(dt_object)
    except:
        print("Exception while parsing weather object")
    return weather

def insert_weather_data(weather):
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        query = "insert into weather(city,state,country,current_temp,max_temp,min_temp,feels_like_temp,pressure,humidity,wind,sunrise,sunset) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(query,tuple(weather))
        conn.commit()
        conn.close()
    except Error as e:
        print("Record Insertion failed with error: "+ str(e))
        return None
    return 1

def loadLocationData():
    key = p2
    try:
        geocoder = OpenCageGeocode(key)
        conn = mysql.connector.connect(host='localhost',database=databaseTask2,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute('SELECT uni_key,institution,country FROM uni_data limit 2400, 4800')   #check from 
        rv = cursor.fetchall()
        for sqlresult in rv:
            results = geocoder.geocode(sqlresult[1])
            queryinput = ()
            if results and len(results):
                city = ""
                state = ""
                country = ""
                if 'city' in results[0]["components"]:
                    city = results[0]["components"]['city']
                if 'state' in results[0]["components"]:
                    state = results[0]["components"]["state"]
                if 'country' in results[0]["components"]:
                    country = results[0]["components"]["country"]
                queryinput = (sqlresult[0],results[0]["geometry"]["lat"],results[0]["geometry"]["lng"],city,state,country,
                results[0]["formatted"],results[0]['annotations']["geohash"],results[0]['annotations']["what3words"]["words"])
                print(queryinput)
                query = "insert into location_task3(uni_key,latitude,longitude,city,state,country,address,geohash,what3words) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(query,tuple(queryinput))
                conn.commit()
            else:
                print("NO OUTPUT for University: "+sqlresult[1])
                loc_dump = (sqlresult[0],sqlresult[1],sqlresult[2])
                query = "insert into location_dump_task3(uni_key,institution,country) values(%s,%s,%s)"
                cursor.execute(query,tuple(loc_dump))
                conn.commit()
            # break
    except Error as e:
        print("Error in loadLocationData: "+str(e))
        pass
    conn.close()

def topological_info():
    city_data = json.loads(getLocationData())
    air_pol_base_url = "http://api.openweathermap.org/data/2.5/air_pollution?lat="
    print("Number of locations in the data source: " + str(len(city_data)))

    if (len(city_data) > 0):
        batchsize = 50
        for j in range(0,len(city_data),batchsize):
            batch = city_data[j:j+batchsize]
            for i in batch:
                complete_url = air_pol_base_url+str(i['latitude'])+"&lon="+str(i['longitude'])+"&appid="+weather_api_key
                print(complete_url)
                # api response
                response = requests.get(complete_url)
                json_response = response.json()
                topology = parse_topological_data(json_response,i['city'],i['state'],i['country'],i['uni_key'])
                db_output = insert_topological_data(topology)
            
            time.sleep(60)

def parse_topological_data(json_response,city,state,country,uni_key):
    topology = [] #city,state,country,aqi,co,nh3,so2,no2,currency,drive_on,speed_in,timezone
    key = p1
    try:
        geocoder = OpenCageGeocode(key)
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute('SELECT institution from uni_data where uni_key='+str(uni_key))
        rv = cursor.fetchall()
        conn.close()
        currency = ""
        drive_on = ""
        speed_in = ""
        results = geocoder.geocode(rv[0][0])
        if results and len(results):
            if 'currency' in results[0]["annotations"]:
                currency = results[0]["annotations"]["currency"]["iso_code"]
            if 'roadinfo' in results[0]["annotations"]:
                drive_on = results[0]["annotations"]["roadinfo"]["drive_on"]
                speed_in = results[0]["annotations"]["roadinfo"]["speed_in"]
            timezone_short = results[0]["annotations"]["timezone"]["short_name"]
        topology.append(city)
        topology.append(state)
        topology.append(country)
        topology.append(json_response["list"][0]["main"]["aqi"])
        topology.append(json_response["list"][0]["components"]["co"])
        topology.append(json_response["list"][0]["components"]["nh3"])
        topology.append(json_response["list"][0]["components"]["so2"])
        topology.append(json_response["list"][0]["components"]["no2"])       
        topology.append(currency)
        topology.append(drive_on)
        topology.append(speed_in)
        topology.append(timezone_short)
    except Error as e:
        print("Exception while parsing topology object"+ str(e))
    return topology

def insert_topological_data(topology):
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        query = "insert into topology(city,state,country,aqi,co,nh3,so2,no2,currency,drive_on,speed_in,timezone) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(query,tuple(topology))
        conn.commit()
        conn.close()
    except Error as e:
        print("Record Insertion failed with error: "+ str(e))
        return None
    return 1

if __name__ == '__main__':
    # weather_info()
    # topological_info()
    # extract_load_uni_Data("2020-QS-World-University-Rankings.csv")
    # loadLocationData()
    app.run(debug=True)