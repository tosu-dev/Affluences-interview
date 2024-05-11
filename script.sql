-- Create history table
CREATE TABLE history(
    id INT not null,
    record_datetime_utc datetime not null,
    entries int not null,
    exits int not null,
    site_id int not null,

    CONSTRAINT HISTORY_PK PRIMARY KEY (id, site_id)
    -- CONSTRAINT FOREIGN KEY (site_id) REFERENCES site(id)
);