from fastapi import FastAPI, HTTPException, Request

app = FastAPI()

@app.post("/submit-data")
async def submit_data(request: Request):
    data = await request.json()  # Access the JSON directly
    token = data.get('token')
    time = data.get('time')
    if not token or not time:
        raise HTTPException(status_code=400, detail="Missing token or time")
    return {"Received Token": token, "Received Time": time}
