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

def parallel_scan(lenderApiKey, tableName, totalsegments, threadsegment):
    
    processdRows = 0
    print("Thread {} started".format(str(threadsegment+1)))

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

    sql = "INSERT INTO NOAAQDescription (SSNHash,DTCreated,BureauID,ClientData,CreditScore,CreditScoreDate,LenderApiKey,MessageID,Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    batch_counter = 1;
    batch_data = []
    filter_expression = "LenderApiKey = :lender_key"
    eav = {":lender_key": lenderApiKey}

    response = table.scan(
        FilterExpression=filter_expression,
        ExpressionAttributeValues=eav,
        TotalSegments=totalsegments,
        Segment=threadsegment,
        Limit=pageSize
    )

    for data in response['Items']:
        dtCreatedObj = parse(data['DTCreated'])
        dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")
        CreditScoreDateString = None

        if 'LenderApiKey'  not in data:
            data['LenderApiKey'] = ''
        if 'BureauID'  not in data:
            data['BureauID'] = ''
        if 'MessageID'  not in data:
            data['MessageID'] = ''
        if 'DTCreated'  not in data:
            data['DTCreated'] = ''
        if 'Status'  not in data:
            data['Status'] = ''
        if 'CreditScoreDate'  not in data:
            data['CreditScoreDate'] = ''
        if 'SSNHash'  not in data:
            data['SSNHash'] = ''
        if 'CreditScore'  not in data:
            data['CreditScore'] = 0
        if 'ClientData'  not in data:
                data['ClientData'] = ''

        if data['CreditScore'] > 0:
            CreditScoreDate = parse(data['CreditScoreDate'])
            CreditScoreDateString = CreditScoreDate.strftime("%Y-%m-%d %H:%M:%S")

        insterData = (data['SSNHash'], dtCreatedString, data['BureauID'], data['ClientData'],data['CreditScore'], CreditScoreDateString, data['LenderApiKey'], data['MessageID'], data['Status'])
        batch_data.append(insterData)

        if batch_counter % batch_size == 0:
            try:                
                dbCusrsor.executemany(sql,batch_data)
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
            dbCusrsor.executemany(sql,batch_data)
            noaaDB.commit()
            processdRows += len(batch_data)
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
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression=filter_expression,
            ExpressionAttributeValues=eav,
            TotalSegments=totalsegments,
            Segment=threadsegment,
            Limit=pageSize
        )

        for data in response['Items']:
            dtCreatedObj = parse(data['DTCreated'])
            dtCreatedString = dtCreatedObj.strftime("%Y-%m-%d %H:%M:%S")
            CreditScoreDateString = None

            if 'LenderApiKey'  not in data:
                data['LenderApiKey'] = ''
            if 'BureauID'  not in data:
                data['BureauID'] = ''
            if 'MessageID'  not in data:
                data['MessageID'] = ''
            if 'DTCreated'  not in data:
                data['DTCreated'] = ''
            if 'Status'  not in data:
                data['Status'] = ''
            if 'CreditScoreDate'  not in data:
                data['CreditScoreDate'] = ''
            if 'SSNHash'  not in data:
                data['SSNHash'] = ''
            if 'CreditScore'  not in data:
                data['CreditScore'] = 0
            if 'ClientData'  not in data:
                data['ClientData'] = ''
            if data['CreditScore'] > 0:
                CreditScoreDate = parse(data['CreditScoreDate'])
                CreditScoreDateString = CreditScoreDate.strftime("%Y-%m-%d %H:%M:%S")

            insterData = (data['SSNHash'], dtCreatedString, data['BureauID'], data['ClientData'],data['CreditScore'], CreditScoreDateString, data['LenderApiKey'], data['MessageID'], data['Status'])
            batch_data.append(insterData)    

            if batch_counter % batch_size == 0:
                try:              
                    dbCusrsor.executemany(sql,batch_data)
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
                    print(err)
            else:
                batch_counter+=1

    if len(batch_data) > 0:
        try:            
            dbCusrsor.executemany(sql,batch_data)
            noaaDB.commit()
            processdRows += len(batch_data)
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
    tablename = "NOAAQDescription"
    total_segments = 10

    lenderApiKeys = ["sGR1AW8T511FWv4wx4xMMaT0HkRGx69x1JeBGn2p",
"cYj0uGwtRH7EF6nrhusD39QjS57dFizk4vgOWX8C",
"DIg9LSP82mavRbODIbZ9C8MLJ3kRS2kygQPeRk29",
"ZPEEmUE1398KEFs2opY7BKdlchozTEy9gi9lHu07",
"bwsGdZ4rpg2PVRBkos23A48j9zazUOJt5qgsWsv9",
"qKWThhGotc2lD8hfd2a2l8qrqVRJfKQEaaDinx0j",
"iBm9m6LTGc7TYbXHnOojU9tKOtUiZaQa2PE2cGqr",
"8P4MEpt4kw5xWxejvUlf9HCB3sOuEOC57OuJv5P2",
"ikP9L2W1ni4P8qFsUTiIS3AszTeaI1tG4pq1AqmZ",
"ozAz9vJCyY7QyfCNVMJOE47V02Cy6I0578ne10jZ",
"hwyDMhNgPv5McYVNdlbtv58Hy4zPz8322LnK0gJX",
"nQaO1qaxzo3pq7RS751PG79wgAROZYs2lUgu14u8",
"ZlHYeWaS5PdwLNxGK3dxarPNYPTxUBK4v7wbyoj5",
"999999999a7nm3SQt7R5d2Bnjc5GChF999999999",
"ksMIg0oCaa7nm3SQt7R5d2Bnjc5GChFq9eloGFuw",
"CAi8tHiYP6aIfyQwOG8S87s43xDYigUf7fgeAW2W",
"AAACCC1",
"AAABBB2",
"FW2zq74yiS8RY7Z1BdbzG8BoT20Pzdps3w2174Ej",
"AAABBB1"]
    
    for apiKey in lenderApiKeys:
        print("Lender api key:{}".format(apiKey))
        print('Script started at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        thread_list = []
        for i in range(total_segments):        
            thread = threading.Thread(target=parallel_scan, args=(apiKey,tablename,total_segments,i))
            thread.start()
            thread_list.append(thread)
            time.sleep(.1)

        for thread in thread_list:
            thread.join()

        print('Script ended at:{}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
