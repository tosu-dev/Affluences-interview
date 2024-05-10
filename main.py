from fastapi import FastAPI

from data import *


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/site/{site_id}")
async def get_site_history_by_id(site_id: int, start_datetime: datetime, end_datetime: datetime):
    json = {"site_id": site_id, "data": [], }

    all_site_df = get_df_every_half_hour(site_id, start_datetime, end_datetime)

    json["data"].append(df_half_hour_to_json(all_site_df[0]))
    old_site_df = all_site_df[0]
    for site_df in all_site_df[1:]:
        json["data"].append(df_half_hour_to_json(site_df, old_site_df))
        old_site_df = site_df
    return json

