import csv
import sys

import pandas as pd
import psycopg2
import requests
import py7zr
import time
import os


#docker-compose build --no-cache && docker-compose up -d --force-recreate
def main():
    start_time = time.time()
    print("Start time: ", time.strftime("%H:%M:%S"))

    # створюємо з'єднання з БД
    conn = createConnection()

    #завантажуємо та розархівовуємо дані про ЗНО з сайту
    download_7z(["2021", "2019"])

    #створюємо датафрейм для зручної праці з даними
    data21 = pd.read_csv('Odata2021File.csv', sep=";", decimal=",", low_memory=False, nrows=10000)
    data19 = pd.read_csv('Odata2019File.csv', sep=";", decimal=",", encoding="Windows-1251", low_memory=False, nrows=10000)
    df19 = pd.DataFrame(data19, columns=['OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME',
                                         'TERNAME', 'REGTYPENAME', 'TerTypeName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName','EOTerName',
                                         'UkrTestStatus',
                                         'UkrBall100',
                                         'UkrBall12',
                                         'UkrBall',
                                         'histTestStatus',
                                         'histBall100',
                                         'histBall12',
                                         'histBall',
                                         'mathTestStatus',
                                         'mathBall100',
                                         'mathBall12',
                                         'mathBall',
                                         'physTestStatus',
                                         'physBall100',
                                         'physBall12',
                                         'physBall',
                                         'chemTestStatus',
                                         'chemBall100',
                                         'chemBall12',
                                         'chemBall',
                                         'bioTestStatus',
                                         'bioBall100',
                                         'bioBall12',
                                         'bioBall',
                                         'geoTestStatus',
                                         'geoBall100',
                                         'geoBall12',
                                         'geoBall',
                                         'engTestStatus',
                                         'engBall100',
                                         'engBall12',
                                         'engBall',
                                         'frTestStatus',
                                         'frBall100',
                                         'frBall12',
                                         'frBall',
                                         'deuTestStatus',
                                         'deuBall100',
                                         'deuBall12',
                                         'deuBall',
                                         'spTestStatus',
                                         'spBall100',
                                         'spBall12',
                                         'spBall',
                                         ])
    df21 = pd.DataFrame(data21, columns=['OUTID', 'Birth', 'SexTypeName', 'RegName', 'AREANAME',
                                         'TERNAME', 'RegTypeName', 'TerTypeName', 'EONAME', 'EOTypeName', 'EORegName','EOAreaName','EOTerName',
                                         'UMLTestStatus',
                                         'UMLBall100',
                                         'UMLBall12',
                                         'UMLBall',
                                         'UkrTestStatus',
                                         'UkrBall100',
                                         'UkrBall12',
                                         'UkrBall',
                                         'HistTestStatus',
                                         'HistBall100',
                                         'HistBall12',
                                         'HistBall',
                                         'MathTestStatus',
                                         'MathBall100',
                                         'MathBall12',
                                         'MathBall',
                                         'PhysTestStatus',
                                         'PhysBall100',
                                         'PhysBall12',
                                         'PhysBall',
                                         'ChemTestStatus',
                                         'ChemBall100',
                                         'ChemBall12',
                                         'ChemBall',
                                         'BioTestStatus',
                                         'BioBall100',
                                         'BioBall12',
                                         'BioBall',
                                         'GeoTestStatus',
                                         'GeoBall100',
                                         'GeoBall12',
                                         'GeoBall',
                                         'EngTestStatus',
                                         'EngBall100',
                                         'EngBall12',
                                         'EngBall',
                                         'FrTestStatus',
                                         'FrBall100',
                                         'FrBall12',
                                         'FrBall',
                                         'DeuTestStatus',
                                         'DeuBall100',
                                         'DeuBall12',
                                         'DeuBall',
                                         'SpTestStatus',
                                         'SpBall100',
                                         'SpBall12',
                                         'SpBall',
                                         ])
    makeZnoGradesNumeric(df19)
    makeZnoGradesNumeric(df21)

    #Якщо таблиці не існує, то створимо її
    if doesTableExist():
        print("Old base")
    else:
        createTable()
        print("New Base")

    #Вставляємо дані в таблицю(комміт через кожні 1000 рядків)
    conn = createConnection()
    insertDataIntoDB(df19, conn, 2019)
    conn = createConnection()
    insertDataIntoDB(df21, conn, 2021)

    #Виконуємо запит згідно 10-го варіанту
    #fetchResultsByRegion("phys", "MAX")

    #Записуємо час виконання програми в текстовий файл
    #txtStopWatch(start_time)
    conn.close()
    print("--- %s seconds ---" % round(time.time() - start_time, 2))


def txtStopWatch(start_time):
    with open('stopwatch.txt', 'w', encoding='UTF-32') as f:
        minutes = int((time.time() - start_time)/60)
        seconds = int((time.time() - start_time) - minutes*60)
        f.write("Time of executing: {0}:{1}".format(minutes, seconds))
        f.close()


def createConnection():
    for attempt in range(15):
        try:
            conn = psycopg2.connect(dbname='student01_DB', user='postgres', password='root1', host='db', port=5432)
            print("Connection to database is successful")
            return conn
        except psycopg2.OperationalError:
            print("Connection failed. Restarting in 4 seconds...")
            time.sleep(4)

    print("Failed to connect. Try later :(")
    sys.exit()


def download_7z(years):
    for year in years:
        if int(year) < 2022:
            url = "https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO" + year + ".7z"
        else:
            url = "https://zno.testportal.com.ua/yearstat/uploads/OpenDataNMT" + year + ".7z"

        req = requests.get(url, stream=True)
        if req.status_code == 200:
            filename = "ZNO" + year
            with open(filename, 'wb') as out:
                out.write(req.content)
            with py7zr.SevenZipFile(filename, 'r') as archive:
                archive.extractall()

            if os.path.isfile("ZNO" + str(year)):
                os.remove("ZNO" + str(year))
        else:
            print('Request failed: %d' % req.status_code)


def doesTableExist():
    conn = createConnection()
    cur = conn.cursor()
    query = """SELECT COUNT(table_name) FROM information_schema.tables
            WHERE table_schema LIKE 'public' AND table_type LIKE 'BASE TABLE' AND table_name = 'zno_records'"""
    cur.execute(query)
    result = cur.fetchall()[0][0]
    if result == 1:
        return True
    return False


def makeZnoGradesNumeric(df):
    for col in df.columns:
        if "Ball100" in col:
            df[col] = df[col].apply(pd.to_numeric)


def createTable():
    conn = createConnection()
    with conn:
        cur = conn.cursor()

        query1 = """
        CREATE TABLE zno_records(
            Year INT,
            OutID VARCHAR(1000) NOT NULL,
            Birth CHAR(4) NOT NULL,
            SexTypeName CHAR(8) NOT NULL,
            Regname VARCHAR(1000) NOT NULL,
            AreaName VARCHAR(1000) NOT NULL,
            TerName VARCHAR(1000) NOT NULL,
            RegTypeName VARCHAR(1000) NOT NULL,
            TerTypeName VARCHAR(1000) NOT NULL,
            EOName VARCHAR(1000),
            EOTypeName VARCHAR(1000),
            EORegName VARCHAR(1000),
            EOAreaName VARCHAR(1000),
            EOTerName VARCHAR(1000),
            UMLTestStatus VARCHAR(25),
            UMLBall100 DECIMAL,
            UMLBall12 DECIMAL,
            UMLBall DECIMAL,
            UkrTestStatus VARCHAR(25),
            UkrBall100 DECIMAL,
            UkrBall12 DECIMAL,
            UkrBall DECIMAL,
            HistTestStatus VARCHAR(25),
            HistBall100 DECIMAL,
            HistBall12 DECIMAL,
            HistBall DECIMAL,
            MathTestStatus VARCHAR(25),
            MathBall100 DECIMAL,
            MathBall12 DECIMAL,
            MathBall DECIMAL,
            PhysTestStatus VARCHAR(25),
            PhysBall100 DECIMAL,
            PhysBall12 DECIMAL,
            PhysBall DECIMAL,
            ChemTestStatus VARCHAR(25),
            ChemBall100 DECIMAL,
            ChemBall12 DECIMAL,
            ChemBall DECIMAL,
            BioTestStatus VARCHAR(25),
            BioBall100 DECIMAL,
            BioBall12 DECIMAL,
            BioBall DECIMAL,
            GeoTestStatus VARCHAR(25),
            GeoBall100 DECIMAL,
            GeoBall12 DECIMAL,
            GeoBall DECIMAL,
            EngTestStatus VARCHAR(25),
            EngBall100 DECIMAL,
            EngBall12 DECIMAL,
            EngBall DECIMAL,
            FrTestStaTus VARCHAR(25),
            FrBall100 DECIMAL,
            FrBall12 DECIMAL,
            FrBall DECIMAL,
            DeuTestStaTus VARCHAR(25),
            DeuBall100 DECIMAL,
            DeuBall12 DECIMAL,
            DeuBall DECIMAL,
            SpTestStaTus VARCHAR(25),
            SpBall100 DECIMAL,
            SpBall12 DECIMAL,
            SpBall DECIMAL 
        );
        """
        cur.execute(query1)


def insertDataIntoDB(df, conn, year):
    columns = [i[0].upper() + i[1:] for i in df.columns]
    values_string = '%s, ' * (len(columns)+1)
    values_string = values_string[:-2]
    columns = "year, " + ', '.join(columns)
    cash = []

    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM zno_records WHERE year=" + str(year))
        num_of_records_to_ignore = cur.fetchall()[0][0]

    except psycopg2.OperationalError:
        conn = createConnection()
        insertDataIntoDB(df, conn, year)

    counter = 0
    for row in df.values:
        row = list(row)
        row.insert(0, year)
        cash.append(row)
        query1 = "INSERT INTO zno_records(" + columns + ") VALUES( " + values_string + ");"
        if counter >= num_of_records_to_ignore:
            try:
                cur = conn.cursor()
                cur.execute(query1, row)
                if counter % 1000 == 0:
                    conn.commit()
                    cash = []
                    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))

            except psycopg2.OperationalError:
                print("Restoring connection...")
                conn = createConnection()
                cur = conn.cursor()
                for el in cash:
                    cur.execute(query1, el)
                cur.execute(query1, row)
                if counter % 1000 == 0:
                    conn.commit()
                    cash = []
                    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))

        counter += 1

    conn.commit()
    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))
    print("Data of {0} year is successfully inserted.".format(year))


def fetchResultsByRegion(subject="phys", func="MAX"):
    query = "SELECT year, regname, "  + func + "(" + subject.capitalize() + "Ball100) FROM zno_records " + "WHERE " + subject + "teststatus='Зараховано' GROUP BY regname, year;"
    conn = createConnection()
    cur = conn.cursor()
    cur.execute(query)
    records = cur.fetchall()
    fieldnames = ['Year', 'Region', subject.capitalize()+"Ball100"]
    rows = []

    for record in records:
        d = {'Year': record[0],
            'Region': record[1],
            subject.capitalize()+"Ball100": record[2]}

        rows.append(d)

    with open('records.csv', 'w', encoding='UTF-32') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        f.close()


main()