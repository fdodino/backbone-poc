CREATE TABLE IF NOT EXISTS employees (
    id         SERIAL         PRIMARY KEY,
    first_name VARCHAR(255)   NOT NULL,
    last_name  VARCHAR(255)   NOT NULL
);
