from fastapi import FastAPI

from data import *
from bonus_data import get_site_history_divide_in_half_hours, get_sum_of_entries_and_exits
from database import get_db

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/site/from-db/{site_id}")
async def get_site_history_from_db_by_id(site_id: int, start_datetime: datetime, end_datetime: datetime):
    """
    Get the history if a site by its id between two datetime with data every half an hour (from the mysql database)
    :param site_id: site's id
    :param start_datetime: beginning datetime
    :param end_datetime: end datetime
    :return: json
    """
    db = get_db()
    site_history = get_site_history_divide_in_half_hours(db, site_id, start_datetime=start_datetime, end_datetime=end_datetime)
    return_json = {"site_id": site_id, "data": [], }
    total_entries = 0
    total_exits = 0
    for current_datetime, history_list in site_history.items():
        entries, exits = get_sum_of_entries_and_exits(history_list)
        history_json = {
            "record_datetime_utc": str((current_datetime.astimezone(tz=timezone.utc) + timedelta(hours=2)).isoformat()),
            "entries": entries,
            "exits": exits,
            "cumulated_entries": entries + total_entries,
            "cumulated_exits": exits + total_exits,
            "occupancy": (entries + total_entries) - (exits + total_exits)
        }
        total_entries += entries
        total_exits += exits
        return_json["data"].append(history_json)

    return return_json


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
