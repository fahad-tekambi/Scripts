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
        host="127.0.0.1",
        user="faisal",
        password="faisal123",
        database="NOAA"
    )

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    dbCusrsor = noaaDB.cursor()

    pageSize = 5000

    response = table.scan(TotalSegments=totalsegments,Segment=threadsegment,Limit=pageSize)

    batch_counter = 1;
    batch_data = []
    sql = "INSERT INTO NOAAQClick2MailLogs (Email,LenderApiKey,DTCreated,NoAddress,SSNHash) VALUES (%s,%s,%s,%s,%s)"
    
    for data in response['Items']:       
        dtCreatedObj = parse(data['DTCreated'])
        dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")

        if 'Email'  not in data:
            data['Email'] = ''
        if 'LenderApiKey'  not in data:
            data['LenderApiKey'] = ''
        if 'DTCreated'  not in data:
            data['DTCreated'] = ''
        if 'NoAddress'  not in data:
            data['NoAddress'] = ''
        if 'SSNHash'  not in data:
            data['SSNHash'] = ''

        insterData = (data['Email'], data['LenderApiKey'],dtCreatedString,data['NoAddress'], data['SSNHash'])    
        batch_data.append(insterData)

        if batch_counter % 10 == 0:
            try:                                
                dbCusrsor.executemany(sql, batch_data)
                noaaDB.commit()
                processdRows +=10
                batch_data.clear()
                batch_counter = 1
                print("Thread {} rows processed:{}".format(str(threadsegment+1),str(processdRows)))

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
        print('Thread:{} calling pagination at:{}'.format(threadsegment,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],TotalSegments=totalsegments,Segment=threadsegment,Limit=pageSize)
        print('Thread:{} got pagination response at:{}'.format(threadsegment,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        for data in response['Items']:            
            dtCreatedObj = parse(data['DTCreated'])
            dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")

            if 'Email'  not in data:
                data['Email'] = ''
            if 'LenderApiKey'  not in data:
                data['LenderApiKey'] = ''
            if 'DTCreated'  not in data:
                data['DTCreated'] = ''
            if 'NoAddress'  not in data:
                data['NoAddress'] = ''
            if 'SSNHash'  not in data:
                data['SSNHash'] = ''

            insterData = (data['Email'], data['LenderApiKey'],dtCreatedString,data['NoAddress'], data['SSNHash'])
            batch_data.append(insterData)

            if batch_counter % 10 == 0:
                try:                                
                    print('Thread:{} batch insertion length:{} starte at:{}'.format(threadsegment,len(batch_data),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    dbCusrsor.executemany(sql, batch_data)
                    noaaDB.commit()
                    print('Thread:{} batch insertion completed at:'.format(threadsegment,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    processdRows += 10
                    batch_data.clear()
                    batch_counter = 1
                    print("Thread {} rows processed inside:{}".format(str(threadsegment+1),str(processdRows)))

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

    noaaDB.close()
    

if __name__ == "__main__":
    tablename = "NOAAQClick2MailLogs"
    total_segments = 5

    print('Script stated at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    begin_time = time.time()

    thread_list = []
    for i in range(total_segments):        
        thread = threading.Thread(target=parallel_scan, args=(tablename,total_segments,i))
        thread.start()
        thread_list.append(thread)
        time.sleep(.1)

    for thread in thread_list:
        thread.join()

    end_time = time.time()

    print('Script ended at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
