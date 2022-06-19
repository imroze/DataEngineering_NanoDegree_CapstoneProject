# DROP TABLES

immigrationfacts_table_drop = "DROP table IF EXISTS immigrationfacts"
visatypes_table_drop = "DROP table IF EXISTS visatypes"
genders_table_drop = "DROP table IF EXISTS genders"
agegroups_table_drop = "DROP table IF EXISTS agegroups"
airports_table_drop = "DROP table IF EXISTS airports"
municipalities_table_drop = "DROP table IF EXISTS municipalities"
years_table_drop = "DROP table IF EXISTS years"
months_table_drop = "DROP table IF EXISTS months"
monthlytimes_table_drop = "DROP table IF EXISTS monthlytimes"
airlines_table_drop = "DROP table IF EXISTS airlines"

# CREATE TABLES

visatypes_table_create = "CREATE TABLE IF NOT EXISTS visatypes \
(visa_code varchar PRIMARY KEY NOT NULL, description varchar);"

genders_table_create = "CREATE TABLE IF NOT EXISTS genders \
(gender_code varchar PRIMARY KEY NOT NULL, description varchar);"

agegroups_table_create = "CREATE TABLE IF NOT EXISTS agegroups \
(agegroup_num int PRIMARY KEY NOT NULL, description varchar);"

municipalities_table_create = "CREATE TABLE IF NOT EXISTS municipalities \
(municipality varchar PRIMARY KEY NOT NULL);"

years_table_create = "CREATE TABLE IF NOT EXISTS years \
(year int PRIMARY KEY NOT NULL);"

months_table_create = "CREATE TABLE IF NOT EXISTS months \
(month_num int PRIMARY KEY NOT NULL, month_name varchar);"

monthlytimes_table_create = "CREATE TABLE IF NOT EXISTS monthlytimes \
(monthlytime varchar PRIMARY KEY NOT NULL, year int, month_num int );"

airports_table_create = "CREATE TABLE IF NOT EXISTS airports \
(airport_code varchar PRIMARY KEY NOT NULL, airport_type varchar, name varchar, municipality varchar );"

airlines_table_create = "CREATE TABLE IF NOT EXISTS airlines \
(airline_code varchar PRIMARY KEY NOT NULL, airline_name varchar, airline_country varchar );"

immigrationfacts_table_create = "CREATE TABLE IF NOT EXISTS immigrationfacts \
(CICID varchar PRIMARY KEY NOT NULL, Year int, Month int, Day int,\
Monthly_Time varchar, Visa_Type varchar, Gender varchar, Age int,\
Age_Group int, Airport_Code varchar, Airport_Municipality varchar,\
Adm_Number varchar, Airline_Code varchar, Flight_Number varchar);"

# INSERT RECORDS

visatypes_table_insert ="INSERT INTO visatypes \
(visa_code,description)\
VALUES (%s,%s) ON CONFLICT DO NOTHING"

genders_table_insert = "INSERT INTO genders \
(gender_code, description)\
VALUES (%s,%s) ON CONFLICT DO NOTHING"

agegroups_table_insert ="INSERT INTO agegroups \
(agegroup_num, description)\
VALUES (%s,%s) ON CONFLICT DO NOTHING"

municipalities_table_insert = "INSERT INTO municipalities \
(municipality)\
VALUES (%s) ON CONFLICT DO NOTHING"

airports_table_insert = "INSERT INTO airports \
(airport_code, airport_type, name, municipality )\
VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"

airlines_table_insert = "INSERT INTO airlines \
(airline_code, airline_name, airline_country )\
VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"

years_table_insert = "INSERT INTO years \
(year)\
VALUES (%s) ON CONFLICT DO NOTHING"

months_table_insert = "INSERT INTO months \
(month_num, month_name)\
VALUES (%s,%s) ON CONFLICT DO NOTHING"

monthlytimes_table_insert = "INSERT INTO monthlytimes \
(monthlytime, year, month_num)\
VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"

immigrationfacts_table_insert = "INSERT INTO immigrationfacts \
(CICID, Year, Month, Day, Monthly_Time, Visa_Type, Gender,\
Age, Age_Group, Airport_Code, Airport_Municipality,\
Adm_Number, Airline_Code, Flight_Number )\
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)\
ON CONFLICT DO NOTHING"

create_table_queries = [visatypes_table_create,genders_table_create,\
                       agegroups_table_create,municipalities_table_create,\
                       years_table_create,months_table_create,\
                       monthlytimes_table_create,airports_table_create,\
                       immigrationfacts_table_create,airlines_table_create]
drop_table_queries = [visatypes_table_drop,genders_table_drop,\
                       agegroups_table_drop,municipalities_table_drop,\
                       years_table_drop,months_table_drop,\
                       monthlytimes_table_drop, airports_table_drop,\
                       immigrationfacts_table_drop, airlines_table_drop]
