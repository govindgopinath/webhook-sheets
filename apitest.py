from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    token: str
    time: str

@app.post("/submit-data")
async def submit_data(item: Item):
    # Process the data as needed, e.g., log it, store it, etc.
    # This example simply echoes the received data back in the response
    return {"Received Token": item.token, "Received Time": item.time}
