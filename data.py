import pandas as pd
from datetime import datetime, timedelta

df = pd.read_csv("dataset.csv")

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def str_to_datetime(str_date: str) -> datetime:
    return datetime.strptime(str_date, DATETIME_FORMAT)


def get_df_by_site_id(site_id: int) -> pd.DataFrame:
    return df.loc[df["site_id"] == site_id].sort_values(by="record_datetime_utc")


def get_df_for_half_hour(site_id: int, start_datetime: datetime) -> pd.DataFrame:
    site_df = get_df_by_site_id(site_id)
    date_serie = pd.to_datetime(site_df["record_datetime_utc"])
    return site_df.loc[(date_serie > start_datetime) & (date_serie < start_datetime + timedelta(minutes=30))]


def get_df_every_half_hour(site_id: int, start_datetime: datetime, end_datetime: datetime) -> list[pd.DataFrame]:
    liste_df = []
    current_datetime = start_datetime
    while current_datetime < end_datetime:
        liste_df.append(get_df_for_half_hour(site_id, current_datetime))
        current_datetime += timedelta(minutes=30)
    return liste_df


def df_half_hour_to_json(df: pd.DataFrame, old_df: pd.DataFrame = None) -> dict:
    if old_df is None:
        old_entries = 0
        old_exists = 0
    else:
        old_entries = old_df.loc[old_df["entries"]].sum()
        old_exists = old_df.loc[old_df["exits"]].sum()

    json = {
        "record_datetime_utc": "", # TODO : parse and convert datetime
        "entries": df.loc[df["entries"]].sum(),
        "exits": df.loc[df["exits"]].sum(),
    }
    json["cumulated_entries"] = json["entries"] + old_entries
    json["cumulated_exits"] = json["exits"] + old_exists
    json["occupancy"] = json["cumulated_entries"] - json["cumulated_exits"]
    return json


if __name__ == "__main__":
    test_df = get_df_every_half_hour(332,
                                     datetime(2024, 4, 21, 0, 30, 0),
                                     datetime(2024, 4, 21, 2, 0, 0))

    print(len(test_df))
