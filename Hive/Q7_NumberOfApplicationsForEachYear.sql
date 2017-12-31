
USE h1b;

INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q7_NumberOfApplicationsForEachYear' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT year,COUNT(*) AS applications FROM h1b_final  GROUP BY year;
