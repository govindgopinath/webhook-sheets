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

data = {
  "mission": {
    "name": "Journey to Mars",
    "spaceship": {
      "name": "Odyssey",
      "crew": [
        {
          "name": "Commander John Smith",
          "role": "Commander",
          "bio": {
            "age": 45,
            "nationality": "American",
            "experience": {
              "years": 20,
              "previous_missions": [
                {
                  "mission_name": "Moon Landing",
                  "year": 2025,
                  "details": {
                    "mission_objective": "Explore lunar south pole",
                    "outcome": {
                      "successful": "true",
                      "discoveries": [
                        {
                          "name": "Water ice",
                          "location": {
                            "latitude": -89.2,
                            "longitude": 45.3,
                            "depth": {
                              "measure": "5 meters",
                              "significance": {
                                "study": {
                                  "title": "Implications of Water Ice on Moon",
                                  "published": "true",
                                  "findings": "Potential for in-situ resource utilization"
                                }
                              }
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              ]
            }
          }
        }
      ],
      "technology": {
        "propulsion": "Ion Thruster",
        "life_support": {
          "systems": {
            "oxygen_generation": {
              "method": "Electrolysis",
              "efficiency": "85%",
              "backup_system": {
                "type": "Chemical oxygen generator",
                "capacity": {
                  "duration": "48 hours",
                  "mechanism": {
                    "activation": "Manual",
                    "control": {
                      "location": "Control Room",
                      "accessibility": {
                        "crew_only": "true",
                        "safety_protocol": {
                          "steps": [
                            "Verify necessity",
                            "Two-person authorization",
                            "Engage system"
                          ]
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}


access_token = "ya29.a0AXooCgvwHh8vFXyT87Z7RKJrwsmE0Qm-8fGtL3N8qPQUgeoK-G6eUl34SvmpV-ryLnKPNEcBe0V87pws-nZvYKNZp8iaGaFYpKNGQKSlaj7fMiA7hNqG0vBrB1eG7OJAZBkMPA2ajkI65uRkdsr_2b37Ky7v4QISLCH0BWhLzxVXYJgocnoaCgYKAbASARESFQHGX2Mi6qVL0vsN405Bp7wY1Qi8tg0186"
spreadsheet_id = "105fr09SfNwsT9v47H6yyQB1joazQjKJ3IhXEm_Pjyn8"
creds = Credentials(token=access_token)
service = build('sheets', 'v4', credentials=creds)

clear_formatting_request ={
    'requests': [
        {
            'unmergeCells': {
                'range': {
                    'sheetId': 0  # Assuming you want to unmerge cells in the first sheet
                }
            }
        }
    ]
}
clear_values_request = {
    'requests': [
        {
            'updateCells': {
                'range': {
                    'sheetId': 0  # Assuming you want to clear the first sheet
                },
                'fields': 'userEnteredValue'
            }
        }
    ]
}
#service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=clear_values_request).execute()
#service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=clear_formatting_request).execute()

def collect_keys(data, level=0, keys_dict=None, z=0, prevkey=""):
    if keys_dict is None:
        keys_dict = {}

    if level not in keys_dict:
        if level>0:
            keys_dict[level] = ['']*len(keys_dict[level-1])
        else:
           keys_dict[level] = [] 

    if isinstance(data, dict):
        for key, value in data.items():
            if prevkey!= "":
                key = prevkey+"char$tGPT"+key
            if level>0:    
                rev_index = keys_dict[level-1][::-1].index(prevkey)
                y = len(keys_dict[level-1])-rev_index-1
                if 'char$tGPT'.join(keys_dict[level][y].split('char$tGPT')[:-1])==prevkey:
                    y = y + 1
                print(rev_index,y)
                print(keys_dict)
                if key not in keys_dict[level]:
                    keys_dict[level].insert(y,key)
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
                    collect_keys(value,level+1,keys_dict,-1,key)
            
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        collect_keys(value[j],level+1,keys_dict,-1,key)

            else: 
                if key not in keys_dict[level]:
                    keys_dict[level].append(key)

                if isinstance(value,dict):
                    collect_keys(value,level+1,keys_dict,-1,key)
            
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        collect_keys(value[j],level+1,keys_dict,-1,key)

    return keys_dict

def format_keys(keys_dict):
    
    max_len = max(len(keys) for keys in keys_dict.values())
    formatted_keys = []

    for level in range(len(keys_dict)):
        keys = keys_dict.get(level, [])
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

@app.post("/sendtoken")
async def receive_token(data: TokenData):
    
    insert_query = """INSERT INTO oauth_token (token,sheetId,tabId,utcTime) VALUES (%s, %s, %s, %s) ON CONFLICT (sheetId, tabId) DO UPDATE SET token = EXCLUDED.token, utcTime = EXCLUDED.utcTime;"""
    data_query = (data.token,data.sheetId,data.tabId,datetime.now())
    conn_sq = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur_sq = conn_sq.cursor()
    cur_sq.execute(insert_query,data_query)
    conn_sq.commit()
    
    return 0



# Collect keys recursively
keys_dict = collect_keys(data)
print(keys_dict)

# Format keys into the desired structure
result = format_keys(keys_dict)

print(getback(result))
requests = merge(format_keys(keys_dict),getback(result))

body = {
    'requests': requests
}

#service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,body=body).execute()
#print(result)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)