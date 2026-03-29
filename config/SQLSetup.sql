CREATE USER 'adapter'@'localhost' IDENTIFIED BY 'ab123';
CREATE DATABASE adapter;
GRANT ALL PRIVILEGES ON adapter.* TO 'adapter'@'localhost';
FLUSH PRIVILEGES;