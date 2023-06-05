from flask import Flask, render_template, url_for, redirect, request
from pymongo import MongoClient

import SQLA_config, mongo_config
from mongo_config import *
import psycopg2
app = Flask(__name__)

#engine = create_engine("postgresql+psycopg2://postgres:root1@localhost/student01_DB")
engine = create_engine("postgresql+psycopg2://postgres:root1@db/student01_DB")

#mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_client = MongoClient('mongodb://mongodb:27017/')

db = mongo_client.student01_DB


#docker-compose build --no-cache && docker-compose up -d --force-recreate
Session = sessionmaker(bind=engine)
session = Session()

Config = SQLA_config.Config


@app.route("/", methods=['POST', 'GET'])
def showTables():
    global Config
    config = Config()
    if request.method == 'POST':
        if request.form["submit_button"] == "Use PostgreSQL":
            Config = SQLA_config.Config
        if request.form["submit_button"] == "Use MongoDB":
            Config = mongo_config.Config

        print(Config)
        if request.form["submit_button"] == "Show locations":
            headers = ("location_id", "regname", "areaname", "tername", "tertypename")
            data = tuple(config.fetchRowsFromLocations())
            return render_template("table.html", headers=headers, data=data, url="/locations")

        elif request.form['submit_button'] == "Show students":
            headers = ("student_id", "year_of_passing", "outid", "birth", "sextypename", "location_id", "eo_id", "tests_results_id")
            data = tuple(config.fetchRowsFromStudents())
            return render_template("table.html", headers=headers, data=data, url="/students")

        elif request.form['submit_button'] == "Show EO":
            headers = ("eo_id", "eo_name", "eo_type", "location_id")
            data = tuple(config.fetchRowsFromEO())
            return render_template("table.html", headers=headers, data=data, url="/eo")

        elif request.form['submit_button'] == "Show Tests":
            headers = config.fetchTestsColumnNames()
            data = tuple(config.fetchRowsFromTests())
            return render_template("table.html", headers=headers, data=data, url="/tests")

        elif request.form['submit_button'] == "Filters":
            years = ("2019", "2021")
            regnames = config.fetchRegnames()
            subjects_dict = config.subjectDict()
            subjects = list(subjects_dict.keys())
            index = 0
            for subject in subjects:
                if subjects[index] == 'Українська мова':
                    subjects[index] = 'Українська_мова'

                if subjects[index] == 'Українська мова та література':
                    subjects[index] = 'Українська_мова_та_література'
                index += 1

            subjects = tuple(subjects)
            functions = ('max', 'min', 'avg')

            return render_template("filters.html", years=years, regnames=regnames, subjects=subjects, functions=functions, url="/filters")

    return render_template("main.html")


@app.route("/locations", methods=['POST', 'GET'])
def locationsTable():
    global Config
    config = Config()
    url="/locations"
    headers = ("location_id", "regname", "areaname", "tername", "tertypename")
    data = tuple(config.fetchRowsFromLocations())
    if request.method == 'POST':
        location_id = request.form['location_id']
        regname = request.form['regname']
        areaname = request.form['areaname']
        tername = request.form['tername']
        tertypename = request.form['tertypename']
        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                config.updateLocation(location_id, regname, areaname, tername, tertypename)
                print(request.form)
            if request.form['update_delete'] == "Delete":
                config.deleteLocation(location_id)

        else:
            config.createLocation(location_id, regname, areaname, tername, tertypename)

        session.commit()
        return redirect(url)

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/eo", methods=['POST', 'GET'])
def eoTable():
    global Config
    config = Config()

    url="/eo"
    headers = ("eo_id", "eo_name", "eo_type", "location_id")
    data = tuple(config.fetchRowsFromEO())
    if request.method == 'POST':
        eo_id = request.form['eo_id']
        eo_name = request.form['eo_name']
        eo_type = request.form['eo_type']
        location_id = request.form['location_id']
        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                config.updateEO(eo_id, eo_name, eo_type, location_id)

            if request.form['update_delete'] == "Delete":
                config.deleteEO(eo_id)

        else:
            config.createEO(eo_id, eo_name, eo_type, location_id)

        session.commit()
        return redirect(url)

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/students", methods=['POST', 'GET'])
def studentsTable():
    global Config
    config = Config()

    url = "/students"
    headers = ("student_id", "year_of_passing", "outid", "birth", "sextypename", "location_id", "eo_id", "tests_results_id")
    data = tuple(config.fetchRowsFromStudents())
    if request.method == 'POST':
        student_id = request.form['student_id']
        year_of_passing = request.form['year_of_passing']
        outid = request.form['outid']
        birth = request.form['birth']
        sextypename = request.form['sextypename']
        location_id = request.form['location_id']
        eo_id = request.form['eo_id']
        tests_results_id = request.form['tests_results_id']
        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                config.updateStudent(student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id)

            if request.form['update_delete'] == "Delete":
                config.deleteStudent(outid)
                session.commit()

        else:
            config.createStudent(student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id)

        session.commit()
        return redirect(url)

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/tests", methods=['POST', 'GET'])
def testsTable():
    global Config
    config = Config()

    url="/tests"
    headers = tuple(config.fetchTestsColumnNames())
    data = tuple(config.fetchRowsFromTests())
    if request.method == 'POST':
        tests_data = {column_name: request.form.get(column_name) if request.form.get(column_name) != '' else None for column_name in headers}
        test = Tests(**tests_data)
        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                config.updateTest(test.tests_id, tests_data)

            if request.form['update_delete'] == "Delete":
                config.deleteTest(test.student_id)

        else:
            config.createTest(tests_data)
        session.commit()
        return redirect(url)

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/filters", methods=["POST", "GET"])
def filters():
    global Config
    config = Config()

    years = ("2019", "2021")
    regnames = config.fetchRegnames()
    subjects_dict = config.subjectDict()
    subjects = list(subjects_dict.keys())
    index = 0
    for subject in subjects:
        if subjects[index] == 'Українська мова':
            subjects[index] = 'Українська_мова'

        if subjects[index] == 'Українська мова та література':
            subjects[index] = 'Українська_мова_та_література'
        index += 1

    subjects = tuple(subjects)
    functions = ('max', 'min', 'avg')
    if request.method == 'POST':
        selected_year = request.form['years']
        selected_regname = request.form['regnames']
        selected_subject = request.form['subjects']
        selected_function = request.form['funcs']

        for key in config.spaceProblemSolverDict().keys():
            if selected_subject == key:
                selected_subject = config.spaceProblemSolverDict().get(key)
        query_result = config.fetchGrade(selected_year, selected_regname, subjects_dict.get(selected_subject), selected_function)
        grade = query_result
        if grade == 0:
            grade = 'None'
        session.commit()

    return render_template("filters.html", years=years, regnames=regnames, subjects=subjects, functions=functions, grade=grade, url="/filters")


if __name__ == "__main__":
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000, debug=True)
