from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine

import json
import pandas as pd
import numpy as np
import pickle
import os.path as path
import sys

SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://guest:relational@relational.fit.cvut.cz:3306/ftp'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
conn = engine.connect()

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello, to get info of views use /session/{session_id}"}

@app.get("/sessions/{session_id}")
def read_item(session_id: str, predict: Optional[str]=None, model: Optional[str]='rfc'):
    result = pd.read_sql_query("""select * 
                from product where session_id = '%s'"""%(session_id), conn)
    res_time = pd.read_sql_query("""select *
                from session where session_id = '%s'"""%(session_id), conn)

    res = np.array(result.nunique().drop(['sequence_order', 'session_id']))

    if (sum(res)==0):
        return """Incorrect session id! 
        Please use id in next format: 'u10000'"""

    if (predict is None):    
        return {"session": session_id, "Counts in categories": json.dumps(res.tolist())}
    elif (predict == 'true'):
        scaler_filename = path.normpath(path.abspath(path.join(__file__,'../../'))+'\\notebooks\scaler_model.sav')
        scaler = pickle.load(open(scaler_filename, 'rb'))

        if (model == 'rfc'):
            model_file = '\\models\\rfc_model.sav'
        elif (model == 'knn'):
            model_file = '\\models\\knn_model.sav'

        model_filename = path.normpath(path.abspath(path\
                                                    .join(__file__,'../../')) + model_file)

        model = pickle.load(open(model_filename, 'rb'))
        
        
        time = (pd.to_datetime(res_time['end_time'])-
                pd.to_datetime(res_time['start_time']))\
                    .apply(lambda i: i.total_seconds() / 60.0)
        res = scaler.transform(np.append(res, time).reshape(1, -1))
        pred = model.predict(res.reshape(1, -1))[0]
        
        if pred == 0:
            pred = 'female'
        else: 
            pred = 'male'
        return {"session": session_id,
               "sex": pred}