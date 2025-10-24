ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'devcontainer';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'devcontainer';
FLUSH PRIVILEGES;


