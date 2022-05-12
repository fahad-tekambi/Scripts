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
    sql = "INSERT INTO NOAAQMessagesLogs (SSNHash,DTCreated,DTOpened,EmailStatus,Error,IPAddress,MessageID,UserAgent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    
    for data in response['Items']:       
        
        if 'EmailStatus'  not in data:
            data['EmailStatus'] = ''
        if 'Error'  not in data:
            data['Error'] = ''                
        if 'IPAddress'  not in data:
            data['IPAddress'] = ''
        if 'SSNHash'  not in data:
            data['SSNHash'] = ''
        if 'MessageID'  not in data:
            data['MessageID'] = ''
        if 'UserAgent'  not in data:
            data['UserAgent'] = ''    

        if 'DTOpened'  not in data:
            dtOpenedString = ''  
        else:
            dtOpenedObj = parse(data['DTOpened'])
            dtOpenedString = dtOpenedObj.strftime("%Y-%m-%d %H:%M:%S")

        if 'DTCreated'  not in data:
            dtCreatedString = ''
        else:
            dtCreatedObj = parse(data['DTCreated'])
            dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")

        insterData = (data['SSNHash'], dtCreatedString,dtOpenedString,data['EmailStatus'], data['Error'], data['IPAddress'],data['MessageID'],data['UserAgent'])    
        batch_data.append(insterData)

        if batch_counter % batch_size == 0:
            try:                                
                dbCusrsor.executemany(sql, batch_data)
                noaaDB.commit()
                processdRows +=batch_size
                batch_data.clear()
                batch_counter = 1

            except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                print("DataError or IntegrityError")
                print(err)
            except mysql.connector.ProgrammingError as err:
                print("Programming Error")
                print(err)
            except mysql.connector.Error as err:
                print("Connection Error")
                print(err)
        else:
            batch_counter +=1
                
    if len(batch_data) > 0:
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

    batch_counter = 1;
    batch_data = []

    while 'LastEvaluatedKey' in response:        
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],TotalSegments=totalsegments,Segment=threadsegment,Limit=pageSize)

        for data in response['Items']:            
            if 'EmailStatus'  not in data:
                data['EmailStatus'] = ''
            if 'Error'  not in data:
                data['Error'] = ''                
            if 'IPAddress'  not in data:
                data['IPAddress'] = ''
            if 'SSNHash'  not in data:
                data['SSNHash'] = ''
            if 'MessageID'  not in data:
                data['MessageID'] = ''
            if 'UserAgent'  not in data:
                data['UserAgent'] = ''    

            if 'DTOpened'  not in data:
                dtOpenedString = ''  
            else:
                dtOpenedObj = parse(data['DTOpened'])
                dtOpenedString = dtOpenedObj.strftime("%Y-%m-%d %H:%M:%S")

            if 'DTCreated'  not in data:
                dtCreatedString = ''
            else:
                dtCreatedObj = parse(data['DTCreated'])
                dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")

            insterData = (data['SSNHash'], dtCreatedString,dtOpenedString,data['EmailStatus'], data['Error'], data['IPAddress'],data['MessageID'],data['UserAgent'])
            batch_data.append(insterData)

            if batch_counter % batch_size == 0:
                try:                                                    
                    dbCusrsor.executemany(sql, batch_data)
                    noaaDB.commit()                    
                    processdRows += batch_size
                    batch_data.clear()
                    batch_counter = 1

                except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                    print("DataError or IntegrityError")
                    print(err)
                except mysql.connector.ProgrammingError as err:
                    print("Programming Error")
                    print(err)
                except mysql.connector.Error as err:
                    print("Connection Error")
                    print(err)
            else:
                batch_counter+=1

    if len(batch_data) > 0:
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

    dbCusrsor.close()
    noaaDB.close()

    print("Thread {} rows processed:{}".format(str(threadsegment+1),str(processdRows)))
    

if __name__ == "__main__":
    tablename = "NOAAQMessagesLogs"
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
