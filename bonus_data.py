from pydantic import BaseModel
from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.orm import Session
from database import get_db

Base = declarative_base()


# ===== MODELS ===== #
class HistoryModel(Base):
    __tablename__ = "history"

    id = Column(Integer, primary_key=True)
    record_datetime_utc = Column(DateTime, nullable=False)
    entries = Column(Integer, nullable=False)
    exits = Column(Integer, nullable=False)
    site_id = Column(Integer, nullable=False)


# ===== SCHEMAS ===== #
class HistoryBase(BaseModel):
    record_datetime_utc: datetime
    entries: int
    exists: int
    site_id: int


class HistoryCreate(HistoryBase):
    pass


class HistorySchema(HistoryBase):
    id: int

    class Config:
        orm_mode = True


# ===== CRUD ===== #
def get_site_history(db: Session, site_id: int, *, start_datetime: datetime = None, end_datetime: datetime = None) -> \
list[HistoryModel]:
    """
    Get the history of a site by its id, between x-30 minutes and x datetime, order by the record datetime
    :param db: the database session
    :param site_id: the site's id
    :param start_datetime: beginning datetime
    :param end_datetime: ending datetime
    :return: list of HistoryModel order by the record datetime and between two datetime
    """
    if (start_datetime is None) or (end_datetime is None):
        return db.query(HistoryModel).filter(HistoryModel.site_id == site_id).order_by(
            HistoryModel.record_datetime_utc).all()
    return (db.query(HistoryModel)
            .filter(HistoryModel.site_id == site_id)
            .filter(HistoryModel.record_datetime_utc > (start_datetime - timedelta(minutes=30)))
            .filter(HistoryModel.record_datetime_utc <= end_datetime)
            .order_by(HistoryModel.record_datetime_utc)
            .all())


def get_site_history_divide_in_half_hours(db: Session, site_id: int, *, start_datetime: datetime = None,
                                          end_datetime: datetime = None) -> dict:
    """
    Get the history of a site by its id, between x-30 minutes and x datetime, order by the record datetime
    and divide the result in half hours
    :param db:
    :param site_id:
    :param start_datetime: beginning datetime
    :param end_datetime: ending datetime
    :return: list of list of HistoryModel
    """
    site_history = get_site_history(db, site_id, start_datetime=start_datetime, end_datetime=end_datetime)
    current_datetime = start_datetime
    histories = {current_datetime: []}
    for i, history in enumerate(site_history):
        # New half hour
        if not (history.record_datetime_utc > (current_datetime - timedelta(minutes=30)) and (
                history.record_datetime_utc <= current_datetime)):
            current_datetime += timedelta(minutes=30)
            histories[current_datetime] = []

        histories[current_datetime].append(history)

    return histories

def get_sum_of_entries_and_exits(histories: list[HistoryModel]):
    """
    Get the sum of all entries and exits in a list of HistoryModel
    :param histories: the list of histories
    :return: (entries, exits)
    """
    return sum([h.entries for h in histories]), sum([h.exits for h in histories])


if __name__ == "__main__":
    db = get_db()
    start_datetime = datetime(2024, 4, 21, 1, 0, 0)
    end_datetime = datetime(2024, 4, 21, 2, 0, 0)
    site_history = get_site_history_divide_in_half_hours(db, 332, start_datetime=start_datetime, end_datetime=end_datetime)
    print(site_history)
    for history_line in site_history.values():
        print(get_sum_of_entries_and_exits(history_line))

