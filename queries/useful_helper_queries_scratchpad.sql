
--- DELETE ALL TABLES FROM DATASET

select concat("drop table `",table_schema,".",   table_name, "`;" )
from DAS_increment.INFORMATION_SCHEMA.TABLES
--where table_name like "INSERT_YOUR_TABLE_NAME_%"
order by table_name desc