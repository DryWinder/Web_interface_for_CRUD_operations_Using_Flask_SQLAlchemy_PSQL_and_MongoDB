from flask import Flask, render_template, url_for, redirect, request
from SQLA_config import *
app = Flask(__name__)

engine = create_engine("postgresql+psycopg2://postgres:root1@localhost/student01_DB")


Session = sessionmaker(bind=engine)
session = Session()


@app.route("/", methods=['POST', 'GET'])
def showTables():
    if request.method == 'POST':
        if request.form["submit_button"] == "Show locations":
            headers = ("location_id", "regname", "areaname", "tername", "tertypename")
            data = tuple(fetchRowsFromLocations())
            return render_template("table.html", headers=headers, data=data, url="/locations")

        elif request.form['submit_button'] == "Show students":
            headers = ("student_id", "year_of_passing", "outid", "birth", "sextypename", "location_id", "eo_id", "tests_results_id")
            data = tuple(fetchRowsFromStudents())
            return render_template("table.html", headers=headers, data=data, url="/students")

        elif request.form['submit_button'] == "Show EO":
            headers = ("eo_id", "eo_name", "eo_type", "location_id")
            data = tuple(fetchRowsFromEO())
            return render_template("table.html", headers=headers, data=data, url="/eo")

        elif request.form['submit_button'] == "Show Tests":
            headers = fetchTestsColumnNames()
            data = tuple(fetchRowsFromTests())
            return render_template("table.html", headers=headers, data=data, url="/tests")

        elif request.form['submit_button'] == "Filters":
            years = ("2019", "2021")
            regnames = (row[0] for row in tuple(fetchRegnames()))
            subjects_dict = subjectDict()
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

        elif request.form["update_delete"] == "Delete":
            pass

    return render_template("main.html")


@app.route("/locations", methods=['POST', 'GET'])
def locationsTable():
    url="/locations"
    headers = ("location_id", "regname", "areaname", "tername", "tertypename")
    data = tuple(fetchRowsFromLocations())
    if request.method == 'POST':
        regname = request.form['regname']
        areaname = request.form['areaname']
        tername = request.form['tername']
        tertypename = request.form['tertypename']
        location = Locations(regname=regname, areaname=areaname, tername=tername, tertypename=tertypename)

        session.add(location)
        session.commit()
        return redirect("/")

    return render_template("table.html", headers=headers, data=data, url=url)

@app.route("/eo", methods=['POST', 'GET'])
def eoTable():
    url="/eo"
    headers = ("eo_id", "eo_name", "eo_type", "location_id")
    data = tuple(fetchRowsFromEO())
    if request.method == 'POST':
        eo_name = request.form['eo_name']
        eo_type = request.form['eo_type']
        location_id = request.form['location_id']
        eo = EO(eo_name=eo_name, eo_type=eo_type, location_id=location_id)

        session.add(eo)
        session.commit()
        return redirect("/")

    return render_template("table.html", headers=headers, data=data, url=url)

@app.route("/students", methods=['POST', 'GET'])
def studentsTable():
    url = "/students"
    headers = ("student_id", "year_of_passing", "outid", "birth", "sextypename", "location_id", "eo_id")
    data = tuple(fetchRowsFromStudents())
    if request.method == 'POST':
        year_of_passing = request.form['year_of_passing']
        outid = request.form['outid']
        birth = request.form['birth']
        sextypename = request.form['sextypename']
        location_id = request.form['location_id']
        eo_id = request.form['eo_id']
        tests_results_id = request.form['tests_results_id']
        student = Students(year_of_passing=year_of_passing, outid=outid, birth=birth, sextypename=sextypename, location_id=location_id, eo_id=eo_id, tests_results_id=tests_results_id)

        session.add(student)
        session.commit()
        return redirect("/")

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/tests", methods=['POST', 'GET'])
def testsTable():
    url="/tests"
    headers = tuple(fetchTestsColumnNames())
    data = tuple(fetchRowsFromTests())
    if request.method == 'POST':
        tests_data = {column_name: request.form.get(column_name) if request.form.get(column_name) != '' else None for column_name in headers}
        test = Tests(**tests_data)

        session.add(test)
        session.commit()
        return redirect("/")

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/filters", methods=["POST", "GET"])
def filters():
    years = ("2019", "2021")
    regnames = (row[0] for row in tuple(fetchRegnames()))
    subjects_dict = subjectDict()
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
    grade = 'None'
    if request.method == 'POST':
        selected_year = request.form['years']
        selected_regname = request.form['regnames']
        selected_subject = request.form['subjects']
        selected_function = request.form['funcs']

        print(tuple(fetchGradesByYearAndRegion()))
        print(selected_subject)
        for key in spaceProblemSolverDict().keys():
            if selected_subject == key:
                selected_subject = spaceProblemSolverDict().get(key)
        print(subjects_dict.get(selected_subject))
        query_result = list(fetchGrade(selected_year, selected_regname, subjects_dict.get(selected_subject), selected_function))
        if len(query_result) != 0:
            grade = query_result[0][2]
        session.commit()

    return render_template("filters.html", years=years, regnames=regnames, subjects=subjects, functions=functions, grade=grade, url="/filters")

if __name__ == "__main__":
    app.run(debug=True)
