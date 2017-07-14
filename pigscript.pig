txn = Load '/home/snv/Downloads/txns.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
--dump txn;
txnbytype = group txn by type;
--dump txnbytype;
totalbytype = foreach txnbytype generate group, ROUND_TO(SUM(txn.amount),2) AS typesales;
--dump totalbytype;
totalgroup = group totalbytype all;
--dump totalgroup;
--describe totalgroup;
--totalgroup: {group: chararray,totalbytype: {(group: bytearray,typesales: double)}}
totalsales = foreach totalgroup generate SUM(totalbytype.typesales) as totalamt;
--dump totalsales;
final = foreach totalbytype generate $0, $1, ROUND_TO((($1*100)/totalsales.totalamt),2);
dump final;

store final into '/home/snv/output10' using PigStorage('\t');
