from pyspark.sql.functions import col, lower,upper,lit, current_date, to_timestamp
from pyspark.sql import SparkSession
import requests
import pyodbc



dba_url = ("jdbc:sqlserver://myserver2026.database.windows.net:1433;database=mydb;user=myadmin@myserver2026;password=Password123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

%sql
SELECT * FROM READ_FILES("/FileStore/tables/fullcardata.csv",
FORMAT=> 'csv',
HEADER=> true,
MODE=>'FAILFAST'
)


%sql
SELECT * FROM READ_FILES("/FileStore/tables/fullcardata.csv",
FORMAT=> 'csv',
HEADER=> true,
MODE=>'FAILFAST'
)

%sql
CREATE TABLE gkamadev.Raw_Cars22 AS
SELECT * FROM READ_FILES("/FileStore/tables/fullcardata.csv",
FORMAT=> 'csv',
HEADER=> true,
MODE=>'FAILFAST'
)
