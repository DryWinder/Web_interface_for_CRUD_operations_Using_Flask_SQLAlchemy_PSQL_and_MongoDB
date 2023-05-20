import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select

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