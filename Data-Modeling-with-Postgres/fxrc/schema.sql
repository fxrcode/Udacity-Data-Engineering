-- https://hashinteractive.com/blog/docker-compose-up-with-postgres-quick-tips/
-- schema.sql
-- Since we might run the import many times we'll drop if exists
DROP DATABASE IF EXISTS blog;

CREATE DATABASE blog;

-- Make sure we're using our `blog` database
\c blog;

-- We can create our user table
CREATE TABLE IF NOT EXISTS app_user (
  id SERIAL PRIMARY KEY,
  username VARCHAR,
  email VARCHAR
);

-- We can create our post table
CREATE TABLE IF NOT EXISTS post (
  id SERIAL PRIMARY KEY,
  userId INTEGER REFERENCES app_user(id),
  title VARCHAR,
  content TEXT,
  image VARCHAR,
  date DATE DEFAULT CURRENT_DATE
);
