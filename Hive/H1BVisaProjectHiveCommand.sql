
Q1 a) Is the number of petitions with Data Engineer job title increasing over time?[ALL]
Ans:- 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q1A_DataEnggJob.sql

-----------------------------------------------------------------------------------------------------------------------------------------------

Q1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q1B_Top5JobTitlesHighAvgGrowth.sql 

-----------------------------------------------------------------------------------------------------------------------------------------------

Q2 b) find top 5 locations in the US who have got certified visa for each year.[certified]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q2B_Top5LocationsInUSGotCertifiedVisaForEahYear.sql 

---------------------------------------------------------------------------------------------------------------------------------------------

3)Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q3_IndustryHasMostDataScientistPositions.sql

---------------------------------------------------------------------------------------------------------------------------------------------

6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.
Ans:- 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q6_PercentCountCaseStatus.sql

-----------------------------------------------------------------------------------------------------------------------------------------------

7) Create a bar graph to depict the number of applications for each year [All]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q7_NumberOfApplicationsForEachYear.sql

---------------------------------------------------------------------------------------------------------------------------------------------

8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order [Certified and Certified Withdrawn.]
Ans:-

Q8 A. For Full Time 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q8A_AvgPrevailingWageForEachJobAndYearFullTime.sql


Q8 B. For Part Time
hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q8B_AvgPrevailingWageForEachJobAndYearPartTime.sql 

----------------------------------------------------------------------------------------------------------------------------------------------

Q 9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?
Ans:- 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q9_EmployersNumberPetitionsSucessRate.sql 

------------------------------------------------------------------------------------------------------------------------------------------------

10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?
Ans:- 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q10_JopPositionsNumberPetitionsSuccessRate.sql
