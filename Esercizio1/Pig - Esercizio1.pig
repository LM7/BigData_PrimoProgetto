myinput = LOAD '/home/lucaubuntu/words.txt' as (text:chararray);
datawords = FOREACH myinput GENERATE FLATTEN(TOKENIZE(text)) as data_words;
DESCRIBE datawords;
words = FILTER datawords BY (NOT(data_words matches '201.*'));
grouped = GROUP words BY $0;
counts = FOREACH grouped GENERATE group, COUNT(words) as cnt;
results = ORDER counts BY cnt DESC;
DUMP results;
store results into '/home/lucaubuntu/pigoutput' using PigStorage();
