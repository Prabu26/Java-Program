cust = Load '/home/snv/Downloads/custs.txt' USING PigStorage(',') AS (custid, firstname, lastname, age:int, profession);
txn = Load '/home/snv/Downloads/txns.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
--dump cust;
txnbygroup = group txn by custid;
--dump txnbygroup;
spendbycust = foreach txnbygroup generate group, ROUND_TO(SUM(txn.amount),2) as total;
--dump spendbycust;
custmorethan500 = filter spendbycust by total > 500;
--dump custmorethan500;
joined = join custmorethan500 by $0, cust by $0;
--dump joined;
agelessthan50 = filter joined by age < 50;
--dump agelessthan50;
final = foreach agelessthan50 generate $0, $3, $4, $5, $6, ROUND_TO($1,2);
dump final;
