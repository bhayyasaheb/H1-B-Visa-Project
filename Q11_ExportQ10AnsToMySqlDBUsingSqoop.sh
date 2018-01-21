


mysql -u root -p'password' -e 'DROP DATABASE h1b;CREATE DATABASE IF NOT EXISTS h1b;USE h1b;CREATE TABLE h1b_final(job_title VARCHAR(255) NOT NULL,petitions INT NOT NULL,success_rate FLOAT NOT NULL);';



#sqoop list-tables --connect jdbc:mysql://localhost/h1b --username root --password root;


sqoop export --connect jdbc:mysql://localhost/h1b --username root --password 'password' --table h1b_final --update-mode  allowinsert --export-dir /H1BVisaProject/Q10_JobPositionsNoPetitionsSuccessRateAbove70Percent/part-r-00000 --input-fields-terminated-by '\t' -m 1 ;

echo -e '\n\n Display content from MySQL Database.\n\n'
echo -e '\n 10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?\n\n'

mysql -u root -p'password' -e 'SELECT * FROM h1b.h1b_final';
