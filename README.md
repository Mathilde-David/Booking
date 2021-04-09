Assignment: ETL pipeline
========================


HOW TO RUN :
============
docker-compose up --build

* If you want to check the data output, in another console tab :

docker-compose exec db psql -localhost -U postgres
\c my_database
\dt
SELECT * FROM monthly_restaurants_report;


MY CHOICES: 
===========
I chose to separate my code in differents parts : 
- data/ : to stock the input file. The output file is saved there too during the pipeline
- transform/ : which contains the function to transform the input dataframe to the output dataframe
- load/ : contains the functions related to the publication of the data on the DB
- test/ : contains a script to test that the input data match the information I've got from them (colnames, number of columns). 
	and a script which tests some potential data quality like the case if some restaurant collect money in different currency or that a country use multiple currencies.
	I choose to assume that the amount in UK must be given in £ using "£XX.XX" formatting and the others country are from UE and use €.
- .env : An environment file with my global variables, I use the same during my local development and in my docker-compose.yml file
- main.py : the ETL script runned. 
	1.	It opens the input file, 
	2.	Tests its struture, 
	3.	Tranforms the input data to get the output data
		Normalize data, format date, manipulate amount string, aggregate values
	4.	Save locally the output data
	5.	Create a connection to the DB
	6.	Check if the table already exists (it does if you don't docker-compose down between two docker-compose up, the DB persists and try to create an existing table could catch an error)
	6 bis. 	If it doesn't : Create the table by executing an SQL query
	7. 	Check if the table is empty (I don't want to put twice the same data in the DB, it could ruin some later analysis)
	7bis. 	If it doesn't : Truncate the table 
		Full truncate works for not incremental data, but the actual dataset could be incremental and add month by month info. 
		In this case it would be more pertinent to delete row where restaurant_id and month match the data to be added, just in case old values would have changed
	8. 	Copy data from output file to the destination table
	9. 	Close DB connection


LIMITS I FACED:
===============
The amount column was quite tricky to manipulate. I try to find an elegant way to handle it using a regex but I add to make some choice at the end. 
I wrote some for loop with a quite big time cost before I found more efficient way to do it using pandas.
I read some documentations on unittest and psycopg2 functions I never used before. 
I had to manage my import/virtual env by myself, I'm used to python interpretor dockerized. 


IMPROVEMENTS:
=============
- Manage the amount column in a more secure way, anticipating for example the use of others currencies like $, maybe by creating a dictionary country-official_currency
- Split in smaller portions my transform script. 
- More exception catcher. Some are missing in the opening/reading steps for example
- More modular code, some variable are hardcoded, like the table destination name for example, it could be more reusable
- Add function to get data from the published table
- Add test to check if all the data is correctly imported
- Use an alpine python image to lighten the docker image size


=========================================================================================================================


INSTRUCTIONS
============
Given the attached dataset (bookings.csv), we want to generate a report with monthly statistics by restaurants.

input dataset : bookings.csv

* booking_id
* restaurant_id
* restaurant_name
* client_id
* client_name
* amount
* Guests (number of people for the given booking)
* date
* country

Expected output dataset  : monthly_restaurants_report.csv

* restaurant_id
* restaurant_name
* country
* month (in following format : YYYY_MM)
* number_of_bookings
* number_of_guests
* amount

The goal of this assignment is to implement this transformation as a proper data engineering pipeline.

Constraints : 

* The final dataset must be dumped in a postgresql table
* The postgresql will be hosted in a docker container

Languages:

 * Python (with any library/framework you want)
 * SQL


It’s simple and relatively unguided on purpose, our criterias are the following : 

* We can make it work
* The output dataset is clean
* The pipeline is cut in well-structured steps, easy to re-run independently easy to maintain and evolve
* The code is clean and well-structured (naming, functions structuration, ...) : imagine you submit this code to your colleagues for review before release
* The code is production-ready (ie. all side aspects needed to industrialize a code : unit tests, exception management, logging, ...)
* Discussion in the README.md : you can write down explanations on how to make the pipeline run arbitrations you took 
* Limitations or things you didn’t have time to implement (we know doing a fully prod-ready script may take quite some time).
* Any extras you think are relevant

