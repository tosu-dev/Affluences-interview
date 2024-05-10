from fastapi import FastAPI

from data import *

app = FastAPI()


@app.get("/")
async def root():
  return {"message": "Hello World"}


@app.get("/site/{site_id}")
async def get_site_history_by_id(site_id: int, start_datetime: datetime, end_datetime: datetime):
  """
  Get the history if a site by its id between two datetime with data every half an hour
  :param site_id: site's id
  :param start_datetime: beginning datetime
  :param end_datetime: end datetime
  :return: json
  """
  # Basic json to return
  return_json = {"site_id": site_id, "data": [], }

  current_datetime = start_datetime
  all_site_df = get_df_every_half_hour(site_id, start_datetime, end_datetime)

  if len(all_site_df) > 0:
    # Init first time
    old_json = df_half_hour_to_json(all_site_df[0], current_datetime)  # Keep old json result
    return_json["data"].append(old_json)  # Put it in the return json
    current_datetime += timedelta(minutes=30)  # Add 30 minutes to current datetime

    # Do this for every other DataFrame
    for site_df in all_site_df[1:]:
      site_json = df_half_hour_to_json(site_df, current_datetime,
                                       total_entries=old_json["cumulated_entries"],
                                       total_exists=old_json["cumulated_exits"])
      return_json["data"].append(site_json)
      old_json = site_json
      current_datetime += timedelta(minutes=30)

  return return_json
