from typing import Any, Dict, List, Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import json
import psycopg2

app = FastAPI()

class TokenData(BaseModel):
    token: str
    sheetId: str
    tabId: str
    email: str

def collect_keys(data, level=0, keys_dict=[[]], prevkey="", colchanges=[]):

    if level>(len(keys_dict)-1):
        if level>0:
            keys_dict.append(['']*len(keys_dict[level-1]))

    print(keys_dict)
    if isinstance(data, dict):
        for key, value in data.items():
            if prevkey!= "":
                key = prevkey+"char$tGPT"+key
            if level>0:    
                if key not in keys_dict[level]:
                    rev_index = keys_dict[level-1][::-1].index(prevkey)
                    y = len(keys_dict[level-1])-rev_index-1
                    if 'char$tGPT'.join(keys_dict[level][y].split('char$tGPT')[:-1])==prevkey:
                        y = y + 1 
                    keys_dict[level].insert(y,key)
                    colchanges.append(y)
                    if len(keys_dict[level])>y:
                        level2=level
                        oldkey = key
                        while level2>0:                                
                            oldkey = 'char$tGPT'.join(oldkey.split('char$tGPT')[:-1])
                            rev_index =  keys_dict[level2-1][::-1].index(oldkey)
                            index = len(keys_dict[level2-1])-rev_index-1
                            m=0
                            times=0
                            while m<len(keys_dict[level2]):
                                if 'char$tGPT'.join(keys_dict[level2][m].split('char$tGPT')[:-1])==oldkey:
                                    times = times + 1
                                m = m+1
                            if times>keys_dict[level2-1].count(oldkey):
                                keys_dict[level2-1].insert(index,oldkey)      
                            level2 = level2-1
                        
                        level2=level
                        while level2<len(keys_dict)-1:
                            keys_dict[level2+1].insert(y,'')
                            level2 = level2+1
                
                if isinstance(value,dict):
                    collect_keys(value,level+1,keys_dict,key,colchanges)
            
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        collect_keys(value[j],level+1,keys_dict,key,colchanges)

            else: 
                if key not in keys_dict[level]:
                    keys_dict[level].append(key)

                if isinstance(value,dict):
                    collect_keys(value,level+1,keys_dict,key,colchanges)
            
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        collect_keys(value[j],level+1,keys_dict,key,colchanges)

    return [keys_dict,colchanges]

def fill_rows(data, level=0, keys_dict=None,row=[],rowlevel=0):
    
    if row == []:
        row.append(['']*len(keys_dict[0]))

    if rowlevel>(len(keys_dict)-1):
        row.append(['']*len(keys_dict[0]))

    if isinstance(data, dict):
        for key, value in data.items():
            if key in keys_dict[level]:
                if isinstance(value,dict):
                    fill_rows(value,level+1,keys_dict,row,rowlevel)
            
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        fill_rows(value[j],level+1,keys_dict,key,row,rowlevel)
                else:
                    index = keys_dict[level].index(key)
                    row[rowlevel][index] = value
    else:
        index = keys_dict[level].index(key)
        row[rowlevel][index] = value

    return row


def format_keys(keys_dict):
    
    max_len = max(len(keys) for keys in keys_dict)
    formatted_keys = []

    for keys in keys_dict:
        while len(keys) < max_len:
            keys.append('')
        formatted_keys.append(keys)
    

    keys = formatted_keys

    y1 = 0
    while y1<len(keys[0]):
        y2 = 0
        counter = 0
        while y2<len(keys):
            if keys[y2][y1]=='':
                counter = counter+1
            y2 = y2 + 1

        if counter==len(keys):
            keys = [[row[i] for i in range(len(row)) if i != y1] for row in keys]
        else:
            y1 = y1 + 1

    return keys 

def getback(keys):
    y1 = 0
    while y1<len(keys):
        y2 = 0
        while y2<len(keys[y1]):
            if keys[y1][y2]!='':
                keys[y1][y2] = keys[y1][y2].split('char$tGPT')[-1]
            y2 = y2 + 1
        y1 = y1 + 1

    return keys    

def merge(keys,keys1):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": 0,
                "endRowIndex": len(keys1),
                "startColumnIndex": 0,
                "endColumnIndex": len(keys1[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": cell}} for cell in row
            ]} for row in keys1
        ],
        "fields": "userEnteredValue"
        }
    })
    y1 = 0
    while y1<len(keys):
        y2 = 0
        count=1
        while y2<len(keys[y1]):
            if y2>0:
                if keys[y1][y2]==keys[y1][y2-1] and keys[y1][y2]!='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y1,
                                    'endRowIndex': y1+1,
                                    'startColumnIndex': y2-count,
                                    'endColumnIndex': y2
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1

        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y1,
                        'endRowIndex': y1+1,
                        'startColumnIndex': y2-count,
                        'endColumnIndex': y2
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
            
        y1 = y1 + 1


    y1 = 0
    while y1<len(keys[0]):
        count = 1
        y2 = 0
        while y2<len(keys):
            if y2>0:
                if keys[y2][y1]=='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y2-count,
                                    'endRowIndex': y2,
                                    'startColumnIndex': y1,
                                    'endColumnIndex': y1+1
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y2-count,
                        'endRowIndex': y2,
                        'startColumnIndex': y1,
                        'endColumnIndex': y1+1
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
        y1 = y1 + 1

    return requests            

def value_merge(rows,pos):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": pos,
                "endRowIndex": pos+len(rows),
                "startColumnIndex": 0,
                "endColumnIndex": len(rows[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": cell}} for cell in row
            ]} for row in rows
        ],
        "fields": "userEnteredValue"
        }
    })

    y1 = 0
    while y1<len(rows[0]):
        count = 1
        y2 = 0
        while y2<len(rows):
            if y2>0:
                if rows[y2][y1]=='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': pos+y2-count,
                                    'endRowIndex': pos+y2,
                                    'startColumnIndex': y1,
                                    'endColumnIndex': y1+1
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': pos+y2-count,
                        'endRowIndex': pos+y2,
                        'startColumnIndex': y1,
                        'endColumnIndex': y1+1
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
        y1 = y1 + 1

    return requests   

@app.post("/sendtoken")
async def receive_token(data: TokenData):
    
    insert_query = """INSERT INTO oauth_token ("token","sheetId","utcTime") VALUES (%s, %s, %s) ON CONFLICT ("sheetId")  DO UPDATE SET "token" = EXCLUDED."token", "utcTime" = EXCLUDED."utcTime";"""
    data_query = (data.token,data.sheetId,datetime.now())
    conn_sq = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur_sq = conn_sq.cursor()
    cur_sq.execute(insert_query,data_query)
    conn_sq.commit()
    
    return 0

@app.post("/datasink/{param:path}")
async def receive_token(param: str, data: Dict):
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()
    query = """SELECT "sheetId", "tabId", "rows" FROM header_structure WHERE "param" = %s;"""
    cur.execute(query, (param,))
    row = cur.fetchone()
    
    if row:
        query = """SELECT "token" FROM oauth_token WHERE "sheetId" = %s;"""
    
        cur.execute(query, (row[0],))
        token = cur.fetchone()
        print(token)
        access_token = token[0]
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)
        
        raw_feed = getdata(token[0],row[0],row[1],row[2])
        
        #existing data
        header = raw_feed[0]
        lastrow = raw_feed[1]

        print(header)
        #new keys cleaning
        results = collect_keys(data,0,header)
        cleaned = format_keys(results[0])
        cleaned_2 = getback(cleaned)
        
        #new data cleaning
        datarow = fill_rows(data,0,results[0])
        print("11")
        if len(cleaned) > int(row[2]):
            requests =  [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": row[1],
                        "dimension": "ROWS",
                        "startIndex": int(row[2]),
                        "endIndex": len(cleaned)+1
                    },
                    "inheritFromBefore": False  # or True depending on context
                }
            }
            ]
            
            body = {
                'requests': requests
            }
            
            #print(body)
            service.spreadsheets().batchUpdate(spreadsheetId=row[0],body=body).execute()


        print(lastrow+len(cleaned)-int(row[2]))

        if (len(results[1])>0): 
            requests = []
            for j in range(len(results[1])):
                requests.append({
                    "insertRange": {
                        "range": {
                            "sheetId": row[1],
                            "startRowIndex": len(cleaned),
                            "endRowIndex": lastrow+len(cleaned)-int(row[2])+1,  
                            "startColumnIndex": results[1][j],  
                            "endColumnIndex": results[1][j]+1,
                        },
                        "shiftDimension": "COLUMNS" 
                    }})
            
            body = {
                'requests': requests
            }
            #print(body)
            service.spreadsheets().batchUpdate(spreadsheetId=row[0],body=body).execute()
        
        print("12")
        #write the header
        requests = merge(cleaned,cleaned_2)

        #write the row
        requests.append(value_merge(datarow,lastrow+len(cleaned)-int(row[2])))

        clear_formatting_request ={
        'requests': [{
            'unmergeCells': {
                'range': {
                    'sheetId': 0  # Assuming you want to unmerge cells in the first sheet
                }}
            }]}
        
        clear_values_request = {
        'requests': [{
            'updateCells': {
                'range': {
                    'sheetId': 0  # Assuming you want to clear the first sheet
                    },
                'fields': 'userEnteredValue'
                }   
            }]}
        
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_values_request).execute()
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_formatting_request).execute()

        print(requests)
        body = {
                'requests': requests
        }
        service.spreadsheets().batchUpdate(spreadsheetId=row[0],body=body).execute()

        if (len(results[0])>int(row[2])):
            query = """UPDATE header_structure SET "rows" = %s WHERE "sheetId" = %s AND "tabId" = %s;"""
            cur.execute(query, (len(results[0]),row[0],row[1]))
            conn.commit()
    
    return 0

def getdata(token,sheetId,tabId,rows):
    
    access_token = token
    spreadsheet_id = sheetId
    creds = Credentials(token=access_token)
    service = build('sheets', 'v4', credentials=creds)

    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_name = None
    for sheet in sheets:
        if sheet['properties']['sheetId'] == int(tabId):
            sheet_name = sheet['properties']['title']
            break
    
    if int(rows)!=0:
        range_name = f'{sheet_name}!1:{rows}' 
        range_all = f'{sheet_name}'
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        result_all = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_all).execute()
        values = result.get('values', [])
        values_all = result_all.get('values', [])

        if values!=[]:
            max_len = max(len(keys) for keys in values)
            formatted_keys = []

            for keys in values:
                while len(keys) < max_len:
                    keys.append('')
                formatted_keys.append(keys)
        
            values = formatted_keys

        y1 = 0
        while y1<len(values):
            y2 = 0
            while y2<len(values[y1]):   
                if values[y1][y2]=='' and y2>0:
                    values[y1][y2] = values[y1][y2-1]
                y2 = y2 + 1
            y1 = y1 + 1
        
        print(values)

        y1 = 0
        while y1<len(values):
            y2 = 0
            while y2<len(values[y1]):   
                if y1>0 and values[y1][y2]!='':
                    print(y1,y2)
                    values[y1][y2] = values[y1-1][y2]+"char$tGPT"+values[y1][y2]
                y2 = y2 + 1
            y1 = y1 + 1
        
    else:
        values = [[]]
        values_all = []

    print(values)    
    return [values,len(values_all)]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)