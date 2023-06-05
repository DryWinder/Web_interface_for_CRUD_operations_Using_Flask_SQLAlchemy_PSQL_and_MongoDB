from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient

from SQLA_config import Students, Locations, EO, Tests

#mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_client = MongoClient('mongodb://mongodb:27017/')

db = mongo_client.student01_DB

#psql_engine = create_engine("postgresql+psycopg2://postgres:root1@localhost/student01_DB")
psql_engine = create_engine("postgresql+psycopg2://postgres:root1@db/student01_DB")
Session = sessionmaker(bind=psql_engine)
session = Session()

locations_collection = db.locations
eo_collection = db.eo
students_collection = db.students
tests_collection = db.tests

class Config:

    def __init__(self):
        if locations_collection.count_documents({}) == 0:
            print("Migrating Students to MongoDB...")
            self.createStudentsCollection()
            print("\nMigrating Locations to MongoDB...")
            self.createLocationsCollection()
            print("\nMigrating EO to MongoDB...")
            self.createEOCollection()
            print("\nMigrating Tests to MongoDB...")
            self.createTestsCollection()

    def createStudentsCollection(self):
        students_collection = db.students
        document_count = students_collection.count_documents({})
        if document_count == 0:
            for student in session.query(Students):
                student_data = {}
                for column in Students.__table__.columns:
                    column_name = column.name
                    column_value = getattr(student, column_name)
                    student_data[column_name] = column_value
                students_collection.insert_one(student_data)


    def createLocationsCollection(self):
        locations_collection = db.locations
        document_count = locations_collection.count_documents({})
        if document_count == 0:
            for location in session.query(Locations):
                location_data = {}
                for column in Locations.__table__.columns:
                    column_name = column.name
                    column_value = getattr(location, column_name)
                    location_data[column_name] = column_value
                locations_collection.insert_one(location_data)


    def createEOCollection(self):
        eo_collection = db.eo
        document_count = eo_collection.count_documents({})
        if document_count == 0:
            for eo in session.query(EO):
                eo_data = {}
                for column in EO.__table__.columns:
                    column_name = column.name
                    column_value = getattr(eo, column_name)
                    eo_data[column_name] = column_value
                eo_collection.insert_one(eo_data)


    def createTestsCollection(self):
        tests_collection = db.tests
        document_count = tests_collection.count_documents({})
        if document_count == 0:
            for tests in session.query(Tests):
                tests_data = {}
                for column in Tests.__table__.columns:
                    column_name = column.name
                    column_value = getattr(tests, column_name)
                    if isinstance(column_value, Decimal):
                        column_value = float(column_value)
                    tests_data[column_name] = column_value
                tests_collection.insert_one(tests_data)


    def dictValuesToTuple(self, dict):
        row = []
        for value in dict.values():
            row.append(value)

        return tuple(row)


    def listOfDictsToTuple(self, query_result):
        result = []
        for dict in list(query_result):
            row = []
            result.append(self.dictValuesToTuple(dict))
        return tuple(result)




    """
    Methods for Locations table
    """


    def fetchRowsFromLocations(self):
        query = locations_collection.find({}, {"_id": 0}).sort("location_id").limit(10)
        result = self.listOfDictsToTuple(query)
        return result


    def fetchLocationsById(self, location_id):
        query = locations_collection.find_one(
            {"location_id": location_id},
            {"_id": 0}
        )
        return self.dictValuesToTuple(query)


    def fetchRegnames(self):
        query = locations_collection.distinct("regname")
        return tuple(query)


    def fetchLocation(self, regname, areaname, tername, tertypename):
        locations_collection = db.locations
        query = locations_collection.find_one({
            "regname": regname,
            "areaname": areaname,
            "tername": tername,
            "tertypename": tertypename
        }, {"_id": 0})
        return self.dictValuesToTuple(query)


    def deleteLocation(self, location_id):
        location_id = int(location_id)
        location = locations_collection.find_one({"location_id": location_id})
        if location:
            locations_collection.delete_one({"location_id": location_id})


    def updateLocation(self, location_id, regname, areaname, tername, tertypename):
        location_id = int(location_id)
        location = locations_collection.find_one({"location_id": location_id})
        if location:
            update_query = {
                "$set": {
                    "regname": regname,
                    "areaname": areaname,
                    "tername": tername,
                    "tertypename": tertypename
                }
            }
            locations_collection.update_one({"location_id": location_id}, update_query)


    def createLocation(self, location_id, regname, areaname, tername, tertypename):
        location_data = {
            "location_id": location_id,
            "regname": regname,
            "areaname": areaname,
            "tername": tername,
            "tertypename": tertypename
        }
        locations_collection.insert_one(location_data)



    """
    Methods for Students table
    """


    def fetchRowsFromStudents(self):
        query = students_collection.find({}, {"_id": 0}).sort("student_id").limit(10)
        result = self.listOfDictsToTuple(query)
        return result


    def fetchStudentsById(self, students_id):
        query = students_collection.find_one(
            {"students_id": students_id},
            {"_id": 0}
        )
        return self.dictValuesToTuple(query)


    def deleteStudent(self, outid):
        student = students_collection.find_one({"outid": outid})
        if student:
            students_collection.delete_one({"outid": outid})


    def createStudent(self, student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id):
        student_data = {
            "student_id": student_id,
            "year_of_passing": year_of_passing,
            "outid": outid,
            "birth": birth,
            "sextypename": sextypename,
            "location_id": location_id,
            "eo_id": eo_id,
            "tests_results_id": tests_results_id
        }
        students_collection.insert_one(student_data)


    def updateStudent(self, student_id, year_of_passing, outid, birth, sextypename, location_id, eo_id, tests_results_id):
        student_id = int(student_id)
        student = students_collection.find_one({"student_id": student_id})
        if student:
            update_query = {
                "$set": {
                    "year_of_passing": year_of_passing,
                    "outid": outid,
                    "birth": birth,
                    "sextypename": sextypename,
                    "location_id": location_id,
                    "eo_id": eo_id,
                    "tests_results_id": tests_results_id
                }
            }
            students_collection.update_one({"student_id": student_id}, update_query)



    """
    Methods for Educational organizations table
    """


    def fetchRowsFromEO(self):
        query = eo_collection.find({}, {"_id": 0}).sort("eo_id").limit(10)
        result = self.listOfDictsToTuple(query)
        return result


    def fetchEOById(self, eo_id):
        query = eo_collection.find_one(
            {"eo_id": eo_id},
            {"_id": 0}
        )
        return self.dictValuesToTuple(query)


    def createEO(self, eo_id, eo_name, eo_type, location_id):
        eo_data = {
            "eo_id": eo_id,
            "eo_name": eo_name,
            "eo_type": eo_type,
            "location_id": location_id
        }
        eo_collection.insert_one(eo_data)


    def deleteEO(self, eo_id):
        eo_id = int(eo_id)
        eo = eo_collection.find_one({"eo_id": eo_id}, {"_id": 0})
        if eo:
            eo_collection.delete_one({"eo_id": eo_id})


    def updateEO(self, eo_id, eo_name, eo_type, location_id):
        eo_id = int(eo_id)
        eo = eo_collection.find_one({"eo_id": eo_id})
        if eo:
            update_query = {
                "$set": {
                    "eo_name": eo_name,
                    "eo_type": eo_type,
                    "location_id": location_id
                }
            }
            eo_collection.update_one({"eo_id": eo_id}, update_query)


    """
    Methods for Tests Results table
    """


    def fetchRowsFromTests(self):
        query = tests_collection.find({}, {"_id": 0}).sort("tests_id").limit(10)
        result = self.listOfDictsToTuple(query)
        return tuple(result)


    def fetchTestsById(self, tests_id):
        query = tests_collection.find_one(
            {"tests_id": tests_id},
            {"_id": 0}
        )
        return self.dictValuesToTuple(query)


    def fetchTestsColumnNames(self):
        column_names = list(tests_collection.find_one().keys())
        column_names.remove('_id')
        return column_names


    def createTest(self, test_data):
        tests_collection.insert_one(test_data)


    def deleteTest(self, student_id):
        student_id = int(student_id)
        test = tests_collection.find_one({"student_id": student_id})
        if test:
            tests_collection.delete_one({"student_id": student_id})


    def updateTest(self, tests_id, test_data):
        tests_id = int(tests_id)
        test = tests_collection.find_one({"tests_id": tests_id})
        if test:
            update_query = {"$set": {}}
            for column_name, value in test_data.items():
                if column_name != "tests_id":
                    if value == 'None':
                        value = None
                    update_query["$set"][column_name] = value
            tests_collection.update_one({"tests_id": tests_id}, update_query)

    def fetchGrade(self, year, regname, subject, function):
        cache_key = f"grade:{year}:{regname}:{subject}:{function}"

        # Перевірка, чи дані є в кеші
        # cached_result = r.get(cache_key)

        # if cached_result is not None:
        # Повернення результату з кешу
        # return json.loads(cached_result)

        if regname != "м.Київ":
            regname += " область"

        pipeline = [
            {
                '$match': {
                    'year_of_passing': 2019
                }
            },
            {
                '$lookup': {
                    'from': 'tests',
                    'localField': 'student_id',
                    'foreignField': 'student_id',
                    'as': 'tests'
                }
            },
            {
                '$lookup': {
                    'from': 'locations',
                    'localField': 'location_id',
                    'foreignField': 'location_id',
                    'as': 'location'
                }
            },
            {
                '$unwind': '$tests'
            },
            {
                '$match': {
                    'location.regname': 'Вінницька область',
                    'tests.math_tests_ball100': {'$exists': True}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'max_math_score': {'$max': '$tests.math_tests_ball100'}
                }
            }
        ]


        print(pipeline)
        result = students_collection.aggregate(pipeline)
        print(list(result))
        grade = 0
        if result:
            for row in result:
                grade = row["grade"]
                # r.setex(cache_key, 7200, float(grade))
                return grade

        # r.setex(cache_key, 7200, float(grade))
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