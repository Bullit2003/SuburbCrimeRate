#########################################################################################################
#   Program Name : CrimeRateDatabase.py                                                                 #
#   Program Description:                                                                                #
#   This program prepares SQLite tables containing data about crime rates in NSW.                       #
#                                                                                                       #
#   Comment                                         Date                  Author                        #
#   ================================                ==========            ================              #
#   Initial Version                                 25/09/2019            Robertus Van Den Braak        #
#########################################################################################################
import sqlite3
from sqlite3 import Error
import csv


def execute_sql(connect, sql_command):
    """ create a table from the create_table_sql statement
    :param connect: Connection object
    :param sql_command: a SQL string
    :return:
    """
    try:
        c = connect.cursor()
        c.execute(sql_command)
    except Error as e:
        print(e)


def retrieve_suburb_id(connect, suburb_name):
    c = connect.cursor()
    c.execute("SELECT SUBURB_ID FROM SUBURB WHERE NAME =?", (suburb_name,))
    suburb_id = c.fetchone()
    if suburb_id is not None:
        return suburb_id[0]


def retrieve_crime_category_id(connect, offence, subcategory):
    c = connect.cursor()
    c.execute("SELECT CRIME_CATEGORY_ID FROM CRIME_CATEGORY WHERE OFFENCE =? AND SUBCATEGORY =?", (offence, subcategory))
    crime_category_id = c.fetchone()
    if crime_category_id is not None:
        return crime_category_id[0]


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def month_num_to_string(month_in):
    month2string = str(month_in)
    if month_in < 10:
        month2string = "0" + month2string
    return month2string

sql_drop_crime_rate_table = """DROP TABLE IF EXISTS CRIME_RATE
                            ; """

sql_drop_crime_category_table = """DROP TABLE IF EXISTS CRIME_CATEGORY
                             ;   """

sql_drop_suburb_table = """DROP TABLE IF EXISTS SUBURB
                        ;   """

sql_create_suburb_table = """ CREATE TABLE SUBURB (
                                    SUBURB_ID INTEGER PRIMARY KEY AUTOINCREMENT, 
                                    NAME varchar(100)
                        ); """

sql_create_crime_category_table = """ CREATE TABLE CRIME_CATEGORY (
                                        CRIME_CATEGORY_ID   INTEGER PRIMARY KEY AUTOINCREMENT,
                                        OFFENCE             varchar(100),
                                        SUBCATEGORY         varchar(200)
                                ); """

sql_create_crime_rate_table = '''   CREATE TABLE CRIME_RATE (
                                            CRIME_RATE_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
                                            SUBURB_ID           int,
                                            CRIME_CATEGORY_ID   int,
                                            START_DATE          datetime,
                                            END_DATE            datetime,
                                            RATE                int,
                                            FOREIGN KEY (SUBURB_ID)
                                                REFERENCES SUBURB (SUBURB_ID)
                                ); '''

#######################################################################
# Create SUBURB_CRIME_RATE Tables                                        #
#######################################################################
conn = sqlite3.connect('SUBURB_CRIME_RATE.sqlite')

# create tables
if conn is not None:
    # clean database
    execute_sql(conn, sql_drop_crime_rate_table)
    execute_sql(conn, sql_drop_crime_category_table)
    execute_sql(conn, sql_drop_suburb_table)

    # create suburb table
    execute_sql(conn, sql_create_suburb_table)

    # create crime category table
    execute_sql(conn, sql_create_crime_category_table)

    # create crime rate table
    execute_sql(conn, sql_create_crime_rate_table)
else:
    print("Error! cannot create the database connection.")

conn.commit()

# Calculate size of data file

row_count = file_len('SuburbCrimeRate.csv')

# Load the CSV file into CSV reader

csvfile = open('SuburbCrimeRate.csv', 'rt')
creader = csv.reader(csvfile, delimiter=',', quotechar='"')

#######################################################################
# Populate Suburb Crime Rate Tables                                   #
#######################################################################
i = 0
for line in creader:
    i += 1
    if i == row_count:
        print('\rProcessing {} of {}'.format(i, row_count))
    else:
        print('\rProcessing {} of {}'.format(i, row_count), end=" ")
    if line[0] != 'Suburb':
        SUBURB_NAME     = line[0].strip()
        OFFENCE         = line[1].strip()
        SUBCATEGORY     = line[2].strip()
        suburbID = retrieve_suburb_id(conn, SUBURB_NAME)
        if suburbID is None:
            conn.execute(''' INSERT INTO SUBURB(NAME) VALUES(?) ''', (SUBURB_NAME,))
            suburbID = retrieve_suburb_id(conn, SUBURB_NAME)
        offenceID = retrieve_crime_category_id(conn, OFFENCE, SUBCATEGORY)
        if offenceID is None:
            conn.execute(''' INSERT INTO CRIME_CATEGORY(OFFENCE, SUBCATEGORY) VALUES(?, ?) ''', (OFFENCE, SUBCATEGORY))
            offenceID = retrieve_crime_category_id(conn, OFFENCE, SUBCATEGORY)
        month = 1
        year = 1995
        n = 3
        for year in range(1995, 2019):
            for month in range(1, 13):
                monthStr = month_num_to_string(month)
                startDate = "{}-{}-01".format(year, monthStr)
                nextMonth = month + 1
                if nextMonth == 13:
                    nextMonth = 1
                    year += 1
                nextMonthStr = month_num_to_string(nextMonth)
                endDate = "{}-{}-01".format(year, nextMonthStr)
                conn.execute(''' INSERT INTO CRIME_RATE (SUBURB_ID, CRIME_CATEGORY_ID, START_DATE, END_DATE, RATE)
                VALUES(?, ?, ?, ?, ?) ''', (suburbID, offenceID, startDate, endDate, line[n]))
                n += 1
print("Creating Indices. Can take up to 4 minutes. Please wait.....")
conn.execute('''CREATE UNIQUE INDEX "IDX_SUBURB_NAME" ON "SUBURB" ("NAME" ASC)''')
conn.execute('''CREATE INDEX "IDX_CRIME_RATE_SUBURB_ID" ON "CRIME_RATE" ("SUBURB_ID" ASC)''')
conn.commit()
csvfile.close()
print('Done!')
