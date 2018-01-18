
--7) Create a bar graph to depict the number of applications for each year [All]

USE h1b;

--Saving output in HDFS
INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q7_NumberOfApplicationsForEachYear' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT year,COUNT(*) AS applications FROM h1b_final  GROUP BY year;


SELECT year,COUNT(*) AS applications FROM h1b_final  GROUP BY year;
--year	applications
--2011	358767
--2013	442114
--2015	618727
--2012	415607
--2014	519427
--2016	647803

