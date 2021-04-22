from flask import Flask, render_template, request,jsonify
import numpy as np
import pandas as pd
import csv
import time
import threading
import re
import requests
import json
from time import gmtime, strftime
import sqlite3

app = Flask(__name__)

def task_post(datay1):
    data1  = {
        "plantCode" :  str(datay1[0]),
        "matCode" :    str(datay1[1]),
        "tankNo" :     str(datay1[2]),
        "dipDate" :    str(datay1[3]),
        "dipTime" :    str(datay1[4]),
        "grossDip" :   str(datay1[5]),
        "waterDip" :   str(datay1[6]),
        "matDensity" : str(datay1[7]),
        "matTemp" :    str(datay1[8]),
        "obsTemp" :    str(datay1[9]),
        "waterSensor" :str(datay1[10]),
        "tankStatus" : str(datay1[11]),
        "dipType" :    str(datay1[12]),
        "bswContent" : str(datay1[13])
        }
    json_String = json.dumps(data1)  
    print(json_String)     
    resp = requests.post('URL',data = json_String,auth = ('USERNAME','PASSWORD'),verify = False)
    fg = resp.content.decode("utf-8")
    print(fg)
    hj = 'nil' 
    if fg:
      fg1 = json.loads(fg)
      hj = fg1['status']      
      print(hj)
    datay1.append(time.strftime('%d-%m-%Y %H:%M:%S') ) 
    datay1.append(hj)
    post_string = datay1
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''INSERT INTO tfms  VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")''' %(post_string[0],post_string[1],post_string[2],post_string[3],post_string[4],post_string[5],post_string[6],post_string[7],post_string[8],post_string[9],post_string[10],post_string[11],post_string[12],post_string[13],post_string[14],post_string[15]))
    conn.commit()
    conn.close() 
    with open("test_posting.csv",'a') as csvfile:
       csvwriter = csv.writer(csvfile,lineterminator='\n')
       csvwriter.writerow(post_string)
    
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/tfmspostinghistory')
def index2():
    return render_template('tfmspostinghistory.html')

@app.route('/update_posting_history')
def indec():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    pxc = c.execute('''SELECT * FROM tfms''').fetchall()
    conn.close()
    df = pd.DataFrame(pxc)
    if(len(df)>0):
        df.columns=['Plant', 'Material','Tank','DipDate','DipTime','GrossDip','WaterDip','MatDensity','MatTemp','ObsTemp','Watersensor','TankStatus','DipType','BswContent','Timestamp','Status']
        df = df[::-1]
        df = df[['Plant', 'Material','Tank','DipDate','DipTime','GrossDip','WaterDip','MatDensity','MatTemp','ObsTemp','TankStatus','DipType','BswContent','Timestamp','Status']]
        tables1=df.to_html(classes='female',index=False)
        update_html = re.sub("class=\"dataframe","class=\"",df.to_html(classes='table table-sm table-bordered female',index=False,justify="center"))
        return jsonify('', render_template('hoddy.html', tables=[update_html])) 
    else:
        return jsonify('', render_template('alert.html', message=["NO POSTINGS FOUND !!","No data .","danger"])) 
   
    
@app.route('/posttfmsdata')
def index4():
     tfmsdata = []
     plantCode = request.args.get('plantCode')
     matCode = request.args.get('matCode')
     tankNo = request.args.get('tankNo')
     grossDip = request.args.get('grossDip')
     waterDip = request.args.get('waterDip')
     matDensity = request.args.get('matDensity')
     matTemp = request.args.get('matTemp')
     obsTemp = request.args.get('obsTemp')
     tankStatus = request.args.get('tankStatus')
     waterSensor = 'C'
     dipType = request.args.get('dipType')
     bswContent = request.args.get('bswContent')
     dipdatetime = request.args.get('dipdatetime') 
     #split date time
     datime = re.split('T',dipdatetime)
     dat = re.split('-',datime[0])
     dipDate = dat[0]+dat[1]+dat[2]
     tim = re.split(':',datime[1])
     dipTime = tim[0]+tim[1]+"00"
     #data to post
     tfmsdata.append(plantCode)
     tfmsdata.append(matCode)
     tfmsdata.append(tankNo)
     tfmsdata.append(dipDate)
     tfmsdata.append(dipTime)
     tfmsdata.append(grossDip)
     tfmsdata.append(waterDip)
     tfmsdata.append(matDensity)
     tfmsdata.append(matTemp)
     tfmsdata.append(obsTemp)
     tfmsdata.append(waterSensor)
     tfmsdata.append(tankStatus)
     tfmsdata.append(dipType)
     tfmsdata.append(bswContent)
     try: 
         threading.Thread(target=task_post(tfmsdata)).start()
         return jsonify('', render_template('alert.html', message=["DATA posted Successfully !!","Kindly check status in posting history.","success"]))  
     except:
         return jsonify('', render_template('alert.html', message=["Something went wrong !!","Data not posted to SAP.","danger"]))  
     

if __name__ == '__main__':
    app.run(host='10.14.111.81',port = 1234 ,debug=True)