The python application consists of two files that require to run in sequential order. But before executing the application it is necessary to extract the SuburbCrimeRate.csv from the SuburbCrimeRate.zip into the running application folder.

Other requirements:
Install Python 3(https://www.python.org/downloads/)

1) In the command prompt navigate to the folder containing the Python files.
2) At the prompt run: python CrimeRateDatabasePrep.py

This will create the SqLite database file, SUBURB_CRIME_RATE.sqlite in the running folder. There are over 276 000 lines in the SuburbCrimeRate.csv file to process into 80 million records so allow 10 minutes for this process to complete. A database that size also needs to be indexed so that will take time to.

3) Once complete at the prompt run: python app.py
You should be able to test the API in a browser using the URL http://127.0.0.1:5000