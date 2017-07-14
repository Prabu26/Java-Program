

visadata = LOAD '/home/hduser/Desktop/jo' USING PigStorage('\t') AS (sno, status, empoyer_name, sco_name, job_title, time_postition, wage:INT, year:INT, worksite, longitude:DOUBLE, latitude:DOUBLE);

scase = FILTER visadata by $1 in ('CERTIFIED' , 'CERTIFIED-WITHDRAWN');

step2 = group scase by $2;

step3 = group visadata by $2;

step4 = FOREACH step2 GENERATE group , COUNT(scase.$0);

step5 = FOREACH step3 GENERATE group , COUNT(visadata.$0);

step6 = JOIN step4 by $0 , step5 by $0;

step7 = FILTER step6 by $1>1000;

step8 = FOREACH step7 GENERATE $0,$1,((double)$1/$3)*100;

step9 = FILTER step8 by $2>70;

dump step9;
