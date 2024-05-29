from typing import Any, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import json
import logging

# Create and configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class TokenData(BaseModel):
    data: Any

# Function to write data to Google Sheet using Google Sheets API.
def write_to_sheet(data: dict, credentials: Credentials):
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    spreadsheet_id = '1A8Mpe-dStdBKjc7DIc4WQSWRm-YK2KFB4o7qGVFBcY8'  # Replace with your actual spreadsheet ID
    range_name = 'Sheet1'  # Replace with your actual sheet name and range
    values = [
        [
            data["Order Code"],
            data["Ticker"],
            data["Sale Date"],
            data["Customer Name"],
            data["Gender"],
            data["City"],
            data["Order Amount"]
        ]
    ]
    body = {
        'values': values
    }
    result = sheet.values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()


def print_json_structure(data: Any, indent: int = 0):
    if isinstance(data, dict):
        for key, value in data.items():
            logger.info('  ' * indent + str(key))
            if isinstance(value, dict):
                print_json_structure(value, indent + 1)
            elif isinstance(value, list):
                logger.info('  ' * (indent + 1) + "List of " + str(len(value)) + " items")
            else:
                logger.info('  ' * (indent + 1) + str(type(value)))
    else:
        logger.error("Provided data is not a dictionary")


@app.post("/receive-token/{param:path}")
async def receive_token(param: str, data: TokenData):
    print(f"Received param: {param}")
    # Prepare the row dat
    row_data = {
        "Order Code": 123456789,
        "Ticker": param,  # Use the param received in the path
        "Sale Date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "Customer Name": "John Doe",
        "Gender": "Male",
        "City": "New York",
        "Order Amount": 1234
    }
    # Load the credentials
    #creds = Credentials(token=data.token)
    logger.info(data.data)
    print_json_structure(data.data)
    # Write the row data to the sheet
    #try:
    #    write_to_sheet(row_data, creds)
    #except Exception as e:
    #    raise HTTPException(status_code=500, detail=str(e))
    return row_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)