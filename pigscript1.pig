txn = Load '/home/snv/Downloads/txns.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
txnbygroup = group txn all;
totalcount = foreach txnbygroup generate COUNT(txn);
dump totalcount;
