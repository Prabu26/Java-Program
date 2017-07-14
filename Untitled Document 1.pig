bag2000 = Load '/home/snv/Downloads/project/sample h1b.csv' USING PigStorage(',') AS (s_no int,case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double )
 
dump bag2000;

combinebag2000 = foreach bag2000 generate $4, $5;

--dump combinebag2000;
