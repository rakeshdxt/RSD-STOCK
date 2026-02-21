import requests
import pandas as pd
import time
import threading
from flask import Flask, send_file

app = Flask(__name__)

CSV_FILE = "nse_data.csv"
progress = {"status":"Idle","percent":0}

# -------- MULTIBAGGER + AI SCORE ----------
def calculate_scores(row):
    score = 0

    if row["roe"] > 15: score += 20
    if row["roce"] > 18: score += 20
    if row["de"] < 0.5: score += 15
    if row["promoter"] > 50: score += 15
    if row["growth"] > 10: score += 15
    if row["pb"] < 3: score += 15

    probability = min(score,100)
    return probability, score


# -------- FETCH DATA ----------
def fetch_data():

    progress["status"]="Fetching"
    symbols = ["RELIANCE","TCS","INFY","HDFCBANK","ITC"]   # add more stocks here

    rows=[]
    total=len(symbols)

    for i,symbol in enumerate(symbols):

        url=f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}.NS"
        r=requests.get(url).json()
        q=r["quoteResponse"]["result"][0]

        row={
            "stock":symbol,
            "price":q.get("regularMarketPrice",0),
            "eps":q.get("epsTrailingTwelveMonths",0),
            "growth":0,
            "mcap":q.get("marketCap",0)/10000000,
            "promoter":50,
            "pledged":0,
            "cfo":0,
            "netprofit":0,
            "roa":0,
            "roce":0,
            "roe":0,
            "pb":q.get("priceToBook",0),
            "de":0
        }

        prob,score=calculate_scores(row)
        row["multibagger_probability"]=prob
        row["ai_score"]=score

        rows.append(row)

        progress["percent"]=int((i+1)/total*100)
        time.sleep(2)

    df=pd.DataFrame(rows)
    df.to_csv(CSV_FILE,index=False)
    progress["status"]="Done"


# -------- SCHEDULER ----------
def scheduler():
    while True:
        t=time.strftime("%H:%M")
        if t in ["09:00","21:00"]:
            fetch_data()
            time.sleep(60)
        time.sleep(20)

threading.Thread(target=scheduler,daemon=True).start()


# -------- API ROUTES ----------
@app.route("/")
def download():
    return send_file(CSV_FILE,as_attachment=True)

@app.route("/progress")
def prog():
    return progress

@app.route("/run-now")
def runnow():
    threading.Thread(target=fetch_data).start()
    return {"status":"Started"}


# -------- START SERVER ----------
if __name__=="__main__":
    app.run(host="0.0.0.0",port=10000)