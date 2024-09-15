CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    balance INT
);

INSERT INTO users (name, balance) VALUES ('Alice', 100), ('Bob', 200);
