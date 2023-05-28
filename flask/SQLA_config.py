from sqlalchemy import create_engine, MetaData, func, distinct
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select
import redis

# 'postgresql+psycopg2://user:password@hostname/database_name'
engine = create_engine("postgresql+psycopg2://postgres:root1@db/student01_DB")


r = redis.Redis(host='redis', port=6379)


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

"""
Methods for Locations table
"""


def fetchRowsFromLocations():
    query = select(Locations).order_by(Locations.location_id).limit(10)
    return conn.execute(query)


def fetchLocationsById(location_id):
    query = session.query(Locations).filter(Locations.location_id == location_id)
    return query.first()


def fetchRegnames():
    query = select(distinct(Locations.regname))
    return conn.execute(query)


def fetchLocation(regname, areaname, tername, tertypename):
    query = session.query(Locations).filter(
        Locations.regname == regname,
        Locations.areaname == areaname,
        Locations.tername == tername,
        Locations.tertypename == tertypename
    )
    return query.first()


def deleteLocation(location_id):
    location = fetchLocationsById(location_id)
    if location:
        session.delete(location)
        session.commit()


def updateLocation(location_id, regname, areaname, tername, tertypename):
    location = fetchLocationsById(location_id)
    if location:
        location.regname = regname
        location.areaname = areaname
        location.tername = tername
        location.tertypename = tertypename
        session.commit()


"""
Methods for Students table
"""


def fetchRowsFromStudents():
    query = select(Students).order_by(Students.student_id).limit(10)
    return conn.execute(query)


def fetchStudentById(student_id):
    query = session.query(Students).filter(Students.student_id == student_id)
    return query.first()


def createStudent(data):
    student = Students(year_of_passing=data['year_of_passing'], outid=data['outid'], birth=data['birth'],
                       sextypename=data['sextypename'], location_id=data['location_id'], eo_id=data['eo_id'])
    session.add(student)
    session.commit()


def updateStudent(student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id):
    student = fetchStudentById(student_id)
    if student:
        student.year_of_passing = year_of_passing
        student.outid = outid
        student.birth = birth
        student.sextypename = sextypename
        student.location_id = location_id
        student.eo_id = eo_id
        student.tests_results_id = tests_results_id
        session.commit()

def deleteStudent(outid):
    student = fetchStudentById(outid)
    if student:
        session.delete(student)
        session.commit()


"""
Methods for Educational organizations table
"""


def fetchRowsFromEO():
    query = select(EO).order_by(EO.eo_id).limit(10)
    return conn.execute(query)


def fetchEOById(eo_id):
    query = session.query(EO).filter(EO.eo_id == eo_id, EO.eo_name != 'NaN')
    return query.first()


def fetchEO(eo_name, eo_type, location_id):
    query = session.query(EO).filter(
        EO.eo_name == eo_name,
        EO.eo_type == eo_type,
        EO.location_id == location_id
    )
    return query.first()


def deleteEO(eo_name, eo_type, location_id):
    eo = fetchEO(eo_name, eo_type, location_id)
    if eo:
        session.delete(eo)
        session.commit()


def updateEO(eo_id, eo_name, eo_type, location_id):
    eo = fetchEOById(eo_id)
    if eo:
        eo.eo_name = eo_name
        eo.eo_type = eo_type
        eo.location_id = location_id
        session.commit()


"""
Methods for Tests Results table
"""


def fetchRowsFromTests():
    query = select(Tests).order_by(Tests.tests_id).limit(10)
    return conn.execute(query)


def fetchTestsColumnNames():
    return Tests.__table__.columns.keys()


def fetchTest(tests_id):
    query = session.query(Tests).filter(Tests.tests_id == tests_id)
    return query.first()


def updateTest(tests_id, test_data):
    test = fetchTest(tests_id)
    if test:
        for column_name, value in test_data.items():
            if value == 'None':
                value = None
            setattr(test, column_name, value)
        session.commit()


def deleteTest(student_id):
    test = fetchTest(student_id)
    if test:
        session.delete(test)
        session.commit()


"""
Different queries
"""



def fetchGrade(year, regname, subject, function):
    cache_key = f"grade:{year}:{regname}:{subject}:{function}"

    # Перевірка, чи дані є в кеші
    cached_result = r.get(cache_key)
    if cached_result is not None:
        # Повернення результату з кешу
        return cached_result.decode('utf-8')
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

    r.set(cache_key, query.all())

    return query.all()


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