
--5) Find the most popular top 10 job positions for H1B visa applications for each year?
--B) for only certified applications.


SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2011' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2012' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2013' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2014' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2015' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

SELECT year,job_title,COUNT(*) AS total FROM h1b_final WHERE year = '2016' AND case_status = 'CERTIFIED' GROUP BY year,job_title ORDER BY total DESC LIMIT 10;

/*
year	job_title	total
2011	PROGRAMMER ANALYST	28806
2011	SOFTWARE ENGINEER	11224
2011	COMPUTER PROGRAMMER	8038
2011	SYSTEMS ANALYST	7850
2011	BUSINESS ANALYST	3444
2011	COMPUTER SYSTEMS ANALYST	3152
2011	ASSISTANT PROFESSOR	3050
2011	PHYSICAL THERAPIST	2911
2011	SENIOR SOFTWARE ENGINEER	2595
2011	SENIOR CONSULTANT	2585

year	job_title	total
2012	PROGRAMMER ANALYST	29226
2012	SOFTWARE ENGINEER	12273
2012	COMPUTER PROGRAMMER	8483
2012	SYSTEMS ANALYST	8399
2012	BUSINESS ANALYST	4144
2012	COMPUTER SYSTEMS ANALYST	4084
2012	SENIOR CONSULTANT	3420
2012	SOFTWARE DEVELOPER	3290
2012	PHYSICAL THERAPIST	3284
2012	ASSISTANT PROFESSOR	3033

year	job_title	total
2013	PROGRAMMER ANALYST	29906
2013	SOFTWARE ENGINEER	12973
2013	COMPUTER PROGRAMMER	10202
2013	SYSTEMS ANALYST	7850
2013	TECHNOLOGY LEAD - US	7809
2013	TECHNOLOGY ANALYST - US	7641
2013	BUSINESS ANALYST	4993
2013	COMPUTER SYSTEMS ANALYST	4554
2013	SOFTWARE DEVELOPER	4316
2013	SENIOR CONSULTANT	3996

year	job_title	total
2014	PROGRAMMER ANALYST	38625
2014	SOFTWARE ENGINEER	17278
2014	COMPUTER PROGRAMMER	13796
2014	SYSTEMS ANALYST	9161
2014	BUSINESS ANALYST	6529
2014	SOFTWARE DEVELOPER	6473
2014	COMPUTER SYSTEMS ANALYST	6204
2014	TECHNOLOGY LEAD - US	5055
2014	TECHNOLOGY ANALYST - US	4911
2014	SENIOR CONSULTANT	4535

year	job_title	total
2015	PROGRAMMER ANALYST	48203
2015	SOFTWARE ENGINEER	23352
2015	COMPUTER PROGRAMMER	12971
2015	SYSTEMS ANALYST	11498
2015	SOFTWARE DEVELOPER	9343
2015	TECHNOLOGY LEAD - US	8238
2015	BUSINESS ANALYST	7919
2015	COMPUTER SYSTEMS ANALYST	7234
2015	TECHNOLOGY ANALYST - US	7009
2015	SENIOR SOFTWARE ENGINEER	5324

year	job_title	total
2016	PROGRAMMER ANALYST	47964
2016	SOFTWARE ENGINEER	25890
2016	SOFTWARE DEVELOPER	12474
2016	SYSTEMS ANALYST	10986
2016	COMPUTER PROGRAMMER	10528
2016	BUSINESS ANALYST	8175
2016	COMPUTER SYSTEMS ANALYST	6205
2016	DEVELOPER	5912
2016	SENIOR SOFTWARE ENGINEER	5630
2016	TECHNOLOGY LEAD - US	5405

*/
