SET GLOBAL autocommit=0;
SET GLOBAL innodb_status_output=ON;
SET GLOBAL innodb_status_output_locks=ON;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    balance INT
);

INSERT INTO users (name, balance) VALUES ('Alice', 100), ('Bob', 200);
