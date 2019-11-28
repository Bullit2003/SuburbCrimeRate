from flask import Flask, g, jsonify, make_response, request
from flask_restplus import Api, Resource
import sqlite3
from os import path

app = Flask(__name__)
api = Api(app, version='1.0', title='Data Service for NSW Crime rate information by suburb',
           description='This is a Flask-Restplus data service that allows a client to consume APIs related to NSW crime rate information by suburb.',
          )

#Database helper
ROOT = path.dirname(path.realpath(__file__))
def connect_db():
    sql = sqlite3.connect(path.join(ROOT, "NSW_CRIME_RATE.sqlite"))
    sql.row_factory = sqlite3.Row
    return sql

def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db

@api.route('/suburb')
class Suburb(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all suburbs from the database.')
    def get(self):
        db = get_db()
        details_cur = db.execute('select SUBURB_ID, NAME from SUBURB')
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['suburbId'] = detail['SUBURB_ID']
            detail_dict['name'] = detail['NAME']\

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/crimeRate/<string:SUBURB>', methods=['GET'])
class CrimeRate(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for all suburb crime rates')
    def get(self, SUBURB):
        db = get_db()
        details_cur = db.execute(
            'SELECT NAME, CRIME_CATEGORY.OFFENCE, CRIME_CATEGORY.SUBCATEGORY, START_DATE, END_DATE, RATE '
            'FROM SUBURB '
            'JOIN CRIME_RATE ON SUBURB.SUBURB_ID = CRIME_RATE.SUBURB_ID '
            'JOIN CRIME_CATEGORY ON CRIME_CATEGORY.CRIME_CATEGORY_ID = CRIME_RATE.CRIME_CATEGORY_ID '
            'WHERE NAME = ? ', [SUBURB])

        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['name'] = detail['NAME']
            detail_dict['offence'] = detail['OFFENCE']
            detail_dict['subcategory'] = detail['SUBCATEGORY']
            detail_dict['startDate'] = detail['START_DATE']
            detail_dict['endDate'] = detail['END_DATE']
            detail_dict['rate'] = detail['RATE']\

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)

@api.route('/crimeRateSummary/<string:SUBURB>', methods=['GET'])
class CrimeRateSummary(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for all suburb crime rates')
    def get(self, SUBURB):
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        db = get_db()
        select = "SELECT NAME, CRIME_CATEGORY.OFFENCE, CRIME_CATEGORY.SUBCATEGORY, SUM(RATE) AS TOTAL_RATE "\
            "FROM SUBURB "\
            "JOIN CRIME_RATE ON SUBURB.SUBURB_ID = CRIME_RATE.SUBURB_ID "\
            "JOIN CRIME_CATEGORY ON CRIME_CATEGORY.CRIME_CATEGORY_ID = CRIME_RATE.CRIME_CATEGORY_ID "\
            "WHERE NAME = '%s' "
        group = "GROUP BY NAME, CRIME_CATEGORY.OFFENCE, CRIME_CATEGORY.SUBCATEGORY"
        if startDate:
            select = select + " AND START_DATE >= '" + startDate + "'"
        if endDate:
            select = select + " AND END_DATE <= '" + endDate + "'"
        sql = select + group
        details_cur = db.execute(sql % (SUBURB))

        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['name'] = detail['NAME']
            detail_dict['offence'] = detail['OFFENCE']
            detail_dict['subcategory'] = detail['SUBCATEGORY']
            detail_dict['rate'] = detail['TOTAL_RATE']\

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)

@api.route('/crimeCategory', methods=['GET'])
class CrimeCategory(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for all crime categories.')
    def get(self):
        db = get_db()
        details_cur = db.execute(
            'select CRIME_CATEGORY_ID, OFFENCE, SUBCATEGORY from CRIME_CATEGORY')
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['crimeCategoryId'] = detail['CRIME_CATEGORY_ID']
            detail_dict['offence'] = detail['OFFENCE']
            detail_dict['subcategory'] = detail['SUBCATEGORY']\

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)

if __name__ == '__main__':
    app.run()
