CREATE TABLE numProdotti (line STRING);
LOAD DATA LOCAL INPATH '/home/sis/hadoop/hive/example_data/products.txt'
OVERWRITE INTO TABLE numProdotti;

CREATE TABLE numProdotti_count AS
SELECT p.product, count(1) AS count FROM
(SELECT explode(split(line, ',')) AS product FROM numProdotti) p
WHERE NOT product LIKE '2%'
GROUP BY p.product
ORDER BY count DESC;

