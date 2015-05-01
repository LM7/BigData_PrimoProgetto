myinput = LOAD '/home/lucaubuntu/words.txt' as (text:chararray);

trimestre = FOREACH (filter myinput by  (INDEXOF(SUBSTRING($0, 4, 6), '01')!=-1 OR INDEXOF(SUBSTRING($0, 4, 6), '02')!=-1 OR INDEXOF(SUBSTRING($0, 4, 6), '03')!=-1)) GENERATE $0;

noGiorno = FOREACH (FOREACH trimestre GENERATE CONCAT(CONCAT(SUBSTRING($0, 0, 6), ','), $0)) GENERATE FLATTEN(STRSPLIT($0, ',')) ;

dataProdotto = FOREACH noGiorno GENERATE $0 AS data, FLATTEN(TOBAG(*)) as value;

data_prodotto = FOREACH (FILTER dataProdotto by INDEXOF(value, '1') == -1) GENERATE  value as prodotto, data;

group_data_prodotto = GROUP data_prodotto BY (prodotto, data);

conteggio = FOREACH group_data_prodotto GENERATE group, COUNT(data_prodotto) as quantity;

tabella = FOREACH conteggio GENERATE $0.prodotto as prodotto, (CONCAT(SUBSTRING($0.data, 0, 4),'/',SUBSTRING($0.data, 4, 6))) as data,  $1 as conteggio_prodotto;

risultato = GROUP tabella BY prodotto;

result= FOREACH risultato {data_quantita = FOREACH tabella GENERATE data, conteggio_prodotto;GENERATE group, data_quantita;}; 

store result into '/home/lucaubuntu/pigoutput2' using PigStorage();
