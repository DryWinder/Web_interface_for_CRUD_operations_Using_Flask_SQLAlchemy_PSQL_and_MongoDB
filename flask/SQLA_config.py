import pandas as pd
from sqlalchemy import create_engine, MetaData, func, distinct
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select
import redis

# 'postgresql+psycopg2://user:password@hostname/database_name'
engine = create_engine("postgresql+psycopg2://postgres:root1@localhost/student01_DB")

Session = sessionmaker(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)

#zno_records = Base.classes.zno_records
Students = Base.classes.students
Locations = Base.classes.locations
EO = Base.classes.educational_organisations
Tests = Base.classes.tests_results




conn = engine.connect()
#result = conn.execute(query)


def fetchRowsFromStudents():
    query = select(Students).limit(10)
    return conn.execute(query)


def fetchRowsFromLocations():
    query = select(Locations).limit(10)
    return conn.execute(query)


def fetchLocationsById(location_id):
    query = session.query(Locations).filter(Locations.location_id == location_id)
    return query.first()


def fetchRowsFromEO():
    query = select(EO).limit(10)
    return conn.execute(query)


def fetchRowsFromTests():
    query = select(Tests).limit(10)
    return conn.execute(query)


def fetchRegnames():
    query = select(distinct(Locations.regname))
    return conn.execute(query)


def fetchGradesByYearAndRegion():
    query = session.query(
        Students.year_of_passing,
        Locations.regname,
        func.max(Tests.uml_test_ball100)
    ).join(
        Locations, Students.location_id == Locations.location_id
    ).join(
        Tests, Students.tests_results_id == Tests.tests_id
    ).filter(
        Tests.uml_test_status == 'Зараховано'
    ).group_by(
        Locations.regname, Students.year_of_passing
    )
    #return conn.execute(query)
    return query.all()


def fetchGrade(year, regname, subject, function):
    if regname != "м.Київ":
        regname += " область"
    query = session.query(
        Students.year_of_passing,
        Locations.regname,
        getattr(func, function)(getattr(Tests, subject))
    ).join(
        Locations, Students.location_id == Locations.location_id
    ).join(
        Tests, Students.tests_results_id == Tests.tests_id
    ).filter(
        getattr(Tests, subject.split("_")[0] + "_test_status") == 'Зараховано',
        Students.year_of_passing == year,
        Locations.regname == regname
    ).group_by(
        Locations.regname, Students.year_of_passing
    )

    return query.all()


def fetchEOById(eo_id):
    query = session.query(EO).filter(EO.eo_id == eo_id)
    return query.first()


def fetchTestsColumnNames():
    return Tests.__table__.columns.keys()

def fetchStudentById(student_id):
    query = session.query(Students).filter(Students.student_id == student_id)
    return query.first()


def createStudent(data):
    student = Students(year_of_passing=data['year_of_passing'], outid=data['outid'], birth=data['birth'],
                       sextypename=data['sextypename'], location_id=data['location_id'], eo_id=data['eo_id'])
    session.add(student)
    session.commit()


def updateStudent(student_id, data):
    student = fetchStudentById(student_id)
    if student:
        student.year_of_passing = data['year_of_passing']
        student.outid = data['outid']
        student.birth = data['birth']
        student.sextypename = data['sextypename']
        student.location_id = data['location_id']
        student.eo_id = data['eo_id']
        session.commit()

def deleteStudent(student_id):
    student = fetchStudentById(student_id)
    if student:
        session.delete(student)
        session.commit()

def subjectDict():
    return {"Українська мова та література": "uml_test_ball100",
                     "Українська мова": "ukr_test_ball100",
                     "Історія України": "hist_test_ball100", "Математика": "math_test_ball100",
                     "Фізика": "phys_test_ball100", "Хімія": "chem_test_ball100",
                     "Географія": "geo_test_ball100",
                     "Англійська мова": "eng_test_ball100", "Французька мова": "fr_test_ball100",
                     "Німецька мова": "deu_test_ball100", "Іспанська мова": "sp_test_ball100"}


def spaceProblemSolverDict():
    return {"Українська_мова_та_література": "Українська мова та література",
     "Українська_мова":"Українська мова",
     "Історія": "Історія України", "Математика": "Математика",
     "Фізика": "Фізика", "Хімія": "Хімія",
     "Географія": "Географія",
     "Англійська": "Англійська мова", "Французька": "Французька мова",
     "Німецька": "Німецька мова", "Іспанська": "Іспанська мова"}