CREATE TABLE prodTrimestre (d STRING, line ARRAY<STRING>) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY ','

LOAD DATA LOCAL INPATH '/home/sis/hadoop/hive/example_data/products.txt'
OVERWRITE INTO TABLE prodTrimestre;

CREATE TABLE prodTrimestre_count AS
SELECT substr(d,0,6) AS date, product, count(1) AS count
FROM prodTrimestre LATERAL VIEW explode(line) adTable AS product
WHERE substr(d,0,6) LIKE '201501' OR substr(d,0,6) LIKE '201502' OR substr(d,0,6) LIKE '201503'
GROUP BY substr(d,0,6),product;

