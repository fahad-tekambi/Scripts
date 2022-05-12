from __future__ import print_function
import boto3
import sys
import time
import threading
from multiprocessing import Queue
from dateutil.parser import parse
from boto3.dynamodb.conditions import Key
from boto.dynamodb2.table import Table
import mysql
import mysql.connector
import datetime

def mysql_batch_insert(batch_data,dbCusrsor,noaaDB):
    sql = "INSERT INTO Tasks (ID,CountProcessed,DTCreated,Status,TaskParams) VALUES (%s,%s,%s,%s,%s)"

    try:       
        dbCusrsor.executemany(sql, batch_data)
        noaaDB.commit()
    except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
        print("DataError or IntegrityError")
        print(err)
    except mysql.connector.ProgrammingError as err:
        print("Programming Error")
        print(err)
    except mysql.connector.Error as err:
        print("Connection Error")
        print(err)


def format_data(data):
    if 'ID'  not in data:
        data['ID'] = ''             
    if 'CountProcessed' not in data:
        data['CountProcessed'] = 0
    if 'DTCreated' not in data:
        data['DTCreated'] = ''
    else:        
        CreditScoreDate = parse(data['DTCreated'])
        data['DTCreated'] = CreditScoreDate.strftime("%Y-%m-%d %H:%M:%S")
    if 'Status'  not in data:
        data['Status'] = 0
    if 'TaskParams'  not in data:
        data['TaskParams'] = ''

    return data

def parallel_scan(tableName, totalsegments, threadsegment):
    
    processdRows = 0
    print("Thread {} stated".format(str(threadsegment+1)))

    noaaDB = mysql.connector.connect(
        host="noaa-mysql-01.cppm7dwiim6o.us-east-1.rds.amazonaws.com",
        user="yasirs",
        password="corporate-assumption-oscillate",
        database="noaa"
    )

    dynamodb = boto3.resource('dynamodb',
        aws_access_key_id='AKIAVJ5B7ACOMD6MGUI5',
        aws_secret_access_key='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',
        region_name='us-east-1'
    )
    
    table = dynamodb.Table(tableName)
    dbCusrsor = noaaDB.cursor()

    pageSize = 5000
    batch_size = 50

    response = table.scan(TotalSegments=totalsegments,Segment=threadsegment,Limit=pageSize)

    batch_counter = 1;
    batch_data = []
    
    for data in response['Items']:        
        formatted_data = format_data(data)        
        insterData = (formatted_data['ID'],data['CountProcessed'], data['DTCreated'], data['Status'],data['TaskParams'])
        batch_data.append(insterData)

        if batch_counter % batch_size == 0:
            mysql_batch_insert(batch_data,dbCusrsor,noaaDB)
            processdRows +=batch_size
            batch_data.clear()
            batch_counter = 1            
        else:
            batch_counter +=1
                
    if len(batch_data) > 0:
        mysql_batch_insert(batch_data,dbCusrsor,noaaDB)

    batch_counter = 1;
    batch_data = []

    while 'LastEvaluatedKey' in response:        
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],TotalSegments=totalsegments,Segment=threadsegment,Limit=pageSize)
        for data in response['Items']:            
            formatted_data = format_data(data)
            insterData = (formatted_data['ID'],data['CountProcessed'], data['DTCreated'], data['Status'],data['TaskParams'])
            batch_data.append(insterData)

            if batch_counter % batch_size == 0:
                mysql_batch_insert(batch_data,dbCusrsor,noaaDB)
                processdRows +=batch_size
                batch_data.clear()
                batch_counter = 1
            else:
                batch_counter+=1

    if len(batch_data) > 0:
        mysql_batch_insert(batch_data,dbCusrsor,noaaDB)       

    dbCusrsor.close()
    noaaDB.close()

    print("Thread {} rows processed:{}".format(str(threadsegment+1),str(processdRows)))
    

if __name__ == "__main__":
    
    tablename = "Tasks"
    total_segments = 10

    print('Script stated at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    thread_list = []
    for i in range(total_segments):        
        thread = threading.Thread(target=parallel_scan, args=(tablename,total_segments,i))
        thread.start()
        thread_list.append(thread)
        time.sleep(.1)

    for thread in thread_list:
        thread.join()

    print('Script ended at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
