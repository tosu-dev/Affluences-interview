import pandas as pd
from datetime import datetime, timedelta, timezone

df = pd.read_csv("dataset.csv")

DATETIME_FORMAT = "%F %T"


def get_df_by_site_id(site_id: int) -> pd.DataFrame:
    """
    Get DataFrame by the site id
    :param site_id: site's id
    :return: DataFrame
    """
    return df.loc[df["site_id"] == site_id].sort_values(by="record_datetime_utc")


def get_df_for_half_hour(site_id: int, end_datetime: datetime) -> pd.DataFrame:
    """
    Get DataFrame between x-30 minutes and x datetime
    :param site_id: site's id
    :param end_datetime: x
    :return: DataFrame
    """
    site_df = get_df_by_site_id(site_id)
    date_serie = pd.to_datetime(site_df["record_datetime_utc"])
    return site_df.loc[(date_serie > end_datetime - timedelta(minutes=30)) & (date_serie <= end_datetime)]


def get_df_every_half_hour(site_id: int, start_datetime: datetime, end_datetime: datetime) -> list[pd.DataFrame]:
    """
    Get DataFrame for every half hour
    :param site_id: site's id
    :param start_datetime: beginning datetime
    :param end_datetime: ending datetime
    :return: list of DataFrame
    """
    liste_df = []
    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        liste_df.append(get_df_for_half_hour(site_id, current_datetime))
        current_datetime += timedelta(minutes=30)
    return liste_df


def df_half_hour_to_json(df: pd.DataFrame, current_datetime: datetime, *, total_entries: int = 0, total_exists: int = 0) -> dict:
    json = {
        # Formating datetime to UTC to ISO 8601 with TimeZone information (+2 hours because french utc)
        "record_datetime_utc": str((current_datetime.astimezone(tz=timezone.utc) + timedelta(hours=2)).isoformat() ),
        # Get entries and exists with sum
        "entries": int(df["entries"].sum()),
        "exits": int(df["exits"].sum()),
    }
    # Caculate cumalted entries and exists , and occupancy
    json["cumulated_entries"] = json["entries"] + total_entries
    json["cumulated_exits"] = json["exits"] + total_exists
    json["occupancy"] = json["cumulated_entries"] - json["cumulated_exits"]
    return json


if __name__ == "__main__":
    # Tests
    test_df = get_df_every_half_hour(332,
                                     datetime(2024, 4, 21, 0, 30, 0),
                                     datetime(2024, 4, 21, 2, 0, 0))

    print(len(test_df))
