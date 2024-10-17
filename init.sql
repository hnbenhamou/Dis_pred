-- init.sql
CREATE TABLE IF NOT EXISTS stock (
    id SERIAL PRIMARY KEY,
    timestamp DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    prediction VARCHAR(255)
);
