# cryptocurrency_analysis
This repository contains python project that will analyse any cryptocurrency relative span and expose it via GET API

Developed on OS: MacOS Mojave 10.14.5
Python version: 3.7.4
Pandas version: 0.24.2

Modules needed
1. sqlite3
2. Pandas
3. Flask


Walkthrough:
Go to the relative span folder and do following steps

Step1 [SETUP]: 
after installing all the above mentioned dependencies execute below command

python3 db_init.py

this will prepare sqlite3 database named crypto.db and creates 2 tables within the database.
1. crypto_daily (to store crypto data with coin and date column added to make the table generic)
2. maxspan (to store the max span that will be used by the GET API)




Step2 [PROCESSING]:
execute below command to calculate the relative spans for each iso week

python3 main.py

above program will use all the custom made modules to calculate the greatest max span and will store it in db table.




Step3 [API]:
execute below to run a flask server

python3 api.py


go to your browser and type

localhost:5000/maxspan

it will result in JSON that will show the year week of greatest maxspan


Thank you!!

