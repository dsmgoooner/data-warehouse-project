/*

========================================================
Create Database for different layers
========================================================

Script Purpose:
	This scripts creates three new database named bronze, silver and gold.
    If the database exists, it is dropped and recreated.
    
*/

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;