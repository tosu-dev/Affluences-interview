import mysql.connector
import sqlalchemy
from sqlalchemy.orm import sessionmaker

DB_HOST: str = "localhost"
DB_PORT: int = 3306
DB_USER: str = "root"
DB_PASSWORD: str = "root"
DB_DATABASE: str = "affluences_db"

db_connect = mysql.connector.connect(
  host=DB_HOST,
  port=DB_PORT,
  user=DB_USER,
  password=DB_PASSWORD,
  database=DB_DATABASE,
)

engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}", echo=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()


if __name__ == "__main__":
    print(db_connect)
    print(engine)



