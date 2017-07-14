bag2000 = Load '/home/snv/Downloads/pig/2000.txt' USING PigStorage(',') AS (catid, catname, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

bag2001 = Load '/home/snv/Downloads/pig/2001.txt' USING PigStorage(',') AS (catid, catname, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

bag2002 = Load '/home/snv/Downloads/pig/2002.txt' USING PigStorage(',') AS (catid, catname, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

combinebag2000 = foreach bag2000 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
combinebag2001 = foreach bag2001 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
combinebag2002 = foreach bag2002 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
--dump combinebag2000;
--dump combinebag2001;
--dump combinebag2002;

joinedbags = JOIN combinebag2000 by $0, combinebag2001 by $0, combinebag2002 by $0;
--dump joinedbags;

joinbyyear = foreach joinedbags generate $0, $1, $2, $5, $8;
--dump joinbyyear;

growth1 = foreach joinbyyear generate $0, $1, $2, $3, $4, (($3-$2)/$2*100), (($4-$3)/$3*100); 
--dump growth1;

ex = group bag2002 by (catid, catname);
--dump ex;

ex1 = foreach ex generate group.catid, group.catname, MAX (bag2002.jan);
dump ex1;
