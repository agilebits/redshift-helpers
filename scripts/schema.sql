-- import_markers table is used to keep track of the most recent successful import for every bucket/table_name
CREATE TABLE import_markers (
	bucket VARCHAR(60) NOT NULL,
    table_name VARCHAR(60) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,

	PRIMARY KEY(bucket, table_name)
);
