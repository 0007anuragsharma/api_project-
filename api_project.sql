--drop database api_database ;

create database api_database;


use api_database;


CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://apiproject0/'
CREDENTIALS = (
    AWS_KEY_ID = 'AKIATT32Z7ISAO27GZES',
    AWS_SECRET_KEY = 'udogIcfa9xVC9pFPa538mO11TUljqAJ5Eks06umr'
)
FILE_FORMAT = (TYPE = JSON);


list@my_s3_stage;



-- drop table bronze_table ;

create or replace table bronze_table (
    raw variant
);



CREATE OR REPLACE PIPE bronze_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = JSON);

-- show pipes like 'bronze_pipe';

select * from bronze_table ;

create or replace stream bronze_stream
on table bronze_table;

select * from bronze_stream;


create or replace table silver_table (
    id number,
    title string,
    price float,
    brand string,
    category string,
    rating float
);







create or replace task silver_load_task
warehouse = compute_wh
schedule = '1 minute'
as
insert into silver_table
select
    value:id::number,
    value:title::string,
    value:price::float,
    value:brand::string,
    value:category::string,
    value:rating::float
from bronze_stream,
lateral flatten(input => raw);

 --alter task silver_to_gold_task resume;
 -- alter task  silver_load_task resume;
-- alter task silver_load_task suspend;


select * from bronze_stream;
 select * from silver_table ;
 




CREATE OR REPLACE TABLE gold_table (
    id NUMBER,
    title STRING,
    price NUMBER(10,2),
    brand STRING,
    category STRING,
    low_rated FLOAT,
    high_rated FLOAT
);

CREATE OR REPLACE STREAM silver_stream
ON TABLE silver_table;


select * from silver_stream;


create or replace task silver_to_gold_task
WAREHOUSE = compute_wh
SCHEDULE = '1 MINUTE'
as
insert into gold_table
select
    id,
    title,
    price,
    brand,
    category,
    coalesce(case when rating < 4 then rating end, 0) as low_rated,
    coalesce(case when rating >= 4 then rating end, 0) as high_rated
from silver_stream
where metadata$action = 'INSERT';




select * from gold_table;

insert into gold_table (id, title, price, brand, category, low_rated, high_rated)
values 
(999, 'Test Product', 50, 'TestBrand', 'beauty', 0, 4.5);


































































--insert into silver_table
--select
--    value:id::number as id,
--    value:title::string as title,
--    value:price::float as price,
 --   value:brand::string as brand,
 --   value:category::string as category,
--    value:rating::float as rating
--from bronze_stream,
--lateral flatten(input => raw);












































































select system$pipe_status('BRONZE_PIPE');

select raw, typeof(raw) from bronze_table limit 5;
