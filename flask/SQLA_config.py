from sqlalchemy import create_engine, MetaData, func, distinct
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select
import redis
import json

# 'postgresql+psycopg2://user:password@hostname/database_name'
#engine = create_engine("postgresql+psycopg2://postgres:root1@localhost/student01_DB")
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


class Config:

    def __init__(self):
        pass
    """
    Methods for Locations table
    """

    def fetchRowsFromLocations(self):
        query = select(Locations).order_by(Locations.location_id).limit(10)
        return conn.execute(query)


    def fetchLocationsById(self, location_id):
        query = session.query(Locations).filter(Locations.location_id == location_id)
        return query.first()


    def fetchRegnames(self):
        query = select(distinct(Locations.regname))
        return tuple(regname[0] for regname in conn.execute(query))


    def fetchLocation(self, regname, areaname, tername, tertypename):
        query = session.query(Locations).filter(
            Locations.regname == regname,
            Locations.areaname == areaname,
            Locations.tername == tername,
            Locations.tertypename == tertypename
        )
        return query.first()


    def deleteLocation(self, location_id):
        location = self.fetchLocationsById(location_id)
        if location:
            session.delete(location)
            session.commit()


    def createLocation(self, location_id, regname, areaname, tername, tertypename):
        location = Locations(regname=regname, areaname=areaname, tername=tername, tertypename=tertypename)
        session.add(location)
        session.commit()


    def updateLocation(self, location_id, regname, areaname, tername, tertypename):
        location = self.fetchLocationsById(location_id)
        if location:
            location.regname = regname
            location.areaname = areaname
            location.tername = tername
            location.tertypename = tertypename
            session.commit()


    """
    Methods for Students table
    """


    def fetchRowsFromStudents(self):
        query = select(Students).order_by(Students.student_id).limit(10)
        return conn.execute(query)


    def fetchStudentById(self, student_id):
        query = session.query(Students).filter(Students.student_id == student_id)
        return query.first()


    def createStudent(self, student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id):
        student = Students(year_of_passing=year_of_passing, outid=outid, birth=birth, sextypename=sextypename,
                           location_id=location_id, eo_id=eo_id, tests_results_id=tests_results_id)
        session.add(student)
        session.commit()


    def updateStudent(self, student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id):
        student = self.fetchStudentById(student_id)
        if student:
            student.year_of_passing = year_of_passing
            student.outid = outid
            student.birth = birth
            student.sextypename = sextypename
            student.location_id = location_id
            student.eo_id = eo_id
            student.tests_results_id = tests_results_id
            session.commit()

    def deleteStudent(self,outid):
        student = self.fetchStudentById(outid)
        if student:
            session.delete(student)
            session.commit()


    """
    Methods for Educational organizations table
    """


    def fetchRowsFromEO(self):
        query = select(EO).order_by(EO.eo_id).limit(10)
        return conn.execute(query)


    def fetchEOById(self, eo_id):
        query = session.query(EO).filter(EO.eo_id == eo_id, EO.eo_name != 'NaN')
        return query.first()


    def fetchEO(self, eo_name, eo_type, location_id):
        query = session.query(EO).filter(
            EO.eo_name == eo_name,
            EO.eo_type == eo_type,
            EO.location_id == location_id
        )
        return query.first()


    def deleteEO(self,eo_name, eo_type, location_id):
        eo = self.fetchEO(eo_name, eo_type, location_id)
        if eo:
            session.delete(eo)
            session.commit()


    def createEO(self, eo_id, eo_name, eo_type, location_id):
        eo = EO(eo_name=eo_name, eo_type=eo_type, location_id=location_id)
        session.add(eo)
        session.commit()

    def updateEO(self, eo_id, eo_name, eo_type, location_id):
        eo = self.fetchEOById(eo_id)
        if eo:
            eo.eo_name = eo_name
            eo.eo_type = eo_type
            eo.location_id = location_id
            session.commit()


    """
    Methods for Tests Results table
    """


    def fetchRowsFromTests(self):
        query = select(Tests).order_by(Tests.tests_id).limit(10)
        return conn.execute(query)


    def fetchTestsColumnNames(self):
        return Tests.__table__.columns.keys()


    def fetchTest(self, tests_id):
        query = session.query(Tests).filter(Tests.tests_id == tests_id)
        return query.first()


    def updateTest(self, tests_id, test_data):
        test = self.fetchTest(tests_id)
        if test:
            for column_name, value in test_data.items():
                if value == 'None':
                    value = None
                setattr(test, column_name, value)
            session.commit()


    def deleteTest(self, student_id):
        test = self.fetchTest(student_id)
        if test:
            session.delete(test)
            session.commit()


    def createTest(self, test_data):
        test = Tests(**test_data)
        session.add(test)
        session.commit()


    """
    Different queries
    """


    def fetchGrade(self, year, regname, subject, function):
        cache_key = f"grade:{year}:{regname}:{subject}:{function}"

        # Перевірка, чи дані є в кеші
        cached_result = r.get(cache_key)

        if cached_result is not None:
            # Повернення результату з кешу
            return json.loads(cached_result)

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

        print(query.all())
        grade = 0
        if len(query.all()) != 0:
            grade = query.all()[0][2]
            r.setex(cache_key, 7200, float(grade))
            return grade

        r.setex(cache_key, 7200, float(grade))
        return grade


    def subjectDict(self):
        return {"Українська мова та література": "uml_test_ball100",
                         "Українська мова": "ukr_test_ball100",
                         "Історія України": "hist_test_ball100", "Математика": "math_test_ball100",
                         "Фізика": "phys_test_ball100", "Хімія": "chem_test_ball100",
                         "Географія": "geo_test_ball100",
                         "Англійська мова": "eng_test_ball100", "Французька мова": "fr_test_ball100",
                         "Німецька мова": "deu_test_ball100", "Іспанська мова": "sp_test_ball100"}


    def spaceProblemSolverDict(self):
        return {"Українська_мова_та_література": "Українська мова та література",
         "Українська_мова":"Українська мова",
         "Історія": "Історія України", "Математика": "Математика",
         "Фізика": "Фізика", "Хімія": "Хімія",
         "Географія": "Географія",
         "Англійська": "Англійська мова", "Французька": "Французька мова",
         "Німецька": "Німецька мова", "Іспанська": "Іспанська мова"}