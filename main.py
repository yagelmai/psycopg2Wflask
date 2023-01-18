from typing import List, Tuple, Any

# from psycopg2 import sql
import psycopg2
import csv
import os
import glob
import time
import pandas

import Utility.DBConnector as Connector
# from Business.Actor import Actor
# from Business.Critic import Critic
# from Business.Movie import Movie
# from Business.Studio import Studio
from Utility.Exceptions import DatabaseException
from Utility.ReturnValue import ReturnValue

from flask import Flask, request


# ---------------------------------- CRUD API: ----------------------------------
app = Flask(__name__)
with app.app_context():

    @app.route('/create_csv_tab')
    def createCsvTables():
        conn = None


        try:
            conn = Connector.DBConnector()

            csvPath = "../out"


            for filename in glob.glob(os.path.join(csvPath,"*.csv")):
            # Create a table name
                tablename = filename.replace("../out", "").replace(".csv", "")[1:-6] + "_csv"
                print(tablename)

                # Open file
                fileInput = open(filename, "r")

                # Extract first line of file
                firstLine = fileInput.readline().strip()

                # Split columns into an array [...]
                columns = firstLine.split(",")

                # Build SQL code to drop table if exists and create table
                sqlQueryCreate = 'DROP TABLE IF EXISTS '+ tablename + ";\n"
                sqlQueryCreate += 'CREATE TABLE '+ tablename + "("

                #some loop or function according to your requiremennt
                # Define columns for table
                for column in columns:
                    sqlQueryCreate += column + " TEXT,\n"

                sqlQueryCreate = sqlQueryCreate[:-2]
                sqlQueryCreate += ");"

                conn.execute(sqlQueryCreate)
                conn.commit()

            #print tables
            _, entries = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            for table in entries.rows:
                print("table name created in postgres: ")
                print(table)
            return {"res": "succ"}
        except Exception as e:
            catchException(e, conn)
            return {"res": "fail"}
        if conn is not None:
            conn.close()

    def fix_list(lst):
        # create a dictionary to store the counts of each element
        element_counts = {}
        fixed_list = []
        for element in lst:
            if element in element_counts:
                element_counts[element] += 1
                fixed_list.append(element + str(element_counts[element]))
            else:
                element_counts[element] = 1
                fixed_list.append(element)
        return fixed_list


    @app.route('/create_map_tab')
    def createMapfileTables():
        conn = None

        try:
            conn = Connector.DBConnector()

            csvPath = "../out"

            for filename in glob.glob(os.path.join(csvPath, "*.mapfile")):
                # Create a table name
                tablename = filename.replace("../out", "").replace(
                    ".mapfile", "")[1:-4] + "_mapfile"
                print(tablename)

                # Open file
                fileInput = open(filename, "r")

                # Extract first line of file
                firstLine = fileInput.readline().strip()

                # Split columns into an array [...]
                columns = firstLine.split(",")

                # Build SQL code to drop table if exists and create table
                sqlQueryCreate = 'DROP TABLE IF EXISTS ' + tablename + ";\n"
                sqlQueryCreate += 'CREATE TABLE ' + tablename + "("

                # some loop or function according to your requiremennt
                # Define columns for table
                columns = fix_list(columns)
                for column in columns:
                    sqlQueryCreate += column + " TEXT,\n"

                sqlQueryCreate = sqlQueryCreate[:-2]
                sqlQueryCreate += ");"

                conn.execute(sqlQueryCreate)
                conn.commit()

            # print tables
            _, entries = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            for table in entries.rows:
                print("table name created in postgres: ")
                print(table)
            return {"res": "succ"}
        except Exception as e:
            catchException(e, conn)
            return {"res": "fail"}
        if conn is not None:
            conn.close()

    def dropPower():
        conn = None
        try:
            conn = Connector.DBConnector()
            conn.execute("DROP TABLE IF EXISTS POWER CASCADE")
        except Exception as e:
            catchException(e, conn)
        if conn is not None:
            conn.close()

    def droprtl():
        conn = None
        try:
            conn = Connector.DBConnector()
            conn.execute("DROP TABLE IF EXISTS RTL CASCADE")
        except Exception as e:
            catchException(e, conn)
        if conn is not None:
            conn.close()

    def dropTables():
        dropPower()
        droprtl()

    def catchException(e: Exception, conn: Any) -> ReturnValue:
        try:
            raise e
        except DatabaseException.ConnectionInvalid as e:
            print(e)
        except DatabaseException.NOT_NULL_VIOLATION as e:
            print(e)
        except DatabaseException.CHECK_VIOLATION as e:
            print(e)
        except DatabaseException.UNIQUE_VIOLATION as e:
            print(e)
        except DatabaseException.FOREIGN_KEY_VIOLATION as e:
            print(e)
        except Exception as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()
            return ReturnValue.OK


    @app.route('/load_csv')
    def copyFromCsvToTable():
        conn = None
        try:
            conn = Connector.DBConnector()
            # Use the COPY statement to copy the contents of the CSV file into the table

            with open('../out/par_exe.power.csv', 'r') as f:
                conn.cursor.copy_expert("COPY par_exe_csv FROM STDIN WITH (FORMAT CSV)", f)
            # Commit the transaction
            conn.commit()
            return {"res": "succ"}

        except Exception as e:
            catchException(e, conn)
            return {"res": "fail"}
        if conn is not None:
            conn.close()


    @app.route('/load_mapfile')
    def copyFromMapfileToTable():
        conn = None
        try:
            conn = Connector.DBConnector()
            # Use the COPY statement to copy the contents of the CSV file into the table

            with open('../out/par_exe.rtl.mapfile', 'r') as f:
                conn.cursor.copy_expert("COPY par_exe_mapfile FROM STDIN WITH (FORMAT CSV)", f)
            # Commit the transaction
            conn.commit()
        except Exception as e:
            catchException(e, conn)
        if conn is not None:
            conn.close()


    @app.route('/join')
    def joinCsvAndMapf():
        conn = None
        try:
            conn = Connector.DBConnector()
            # Use the COPY statement to copy the contents of the CSV file into the table
            query = psycopg2.sql.SQL(
            "CREATE TABLE csvJoinMap AS "
            "SELECT par_exe_csv.*, par_exe_mapfile.* "
            "FROM par_exe_csv "
            "INNER JOIN par_exe_mapfile "
            "ON (par_exe_csv.cell_name LIKE '%' || par_exe_mapfile.dlvrloadgndvsense0|| '%') "
            "LIMIT 100")
            conn.execute(query)

            # Commit the transaction
            conn.commit()
            return {"res": "succ"}
        except Exception as e:
            catchException(e, conn)
            return {"res": "fail"}
        if conn is not None:
            conn.close()


    @app.route('/createDF')
    def getTableAsDF():
        conn = None
        try:
            conn = Connector.DBConnector()
            # Use the COPY statement to copy the contents of the CSV file into the table
            query = psycopg2.sql.SQL(
                "SELECT * FROM csvjoinmap LIMIT 11"
            )
            _, res = conn.execute(query)
            df = pandas.DataFrame(res.rows, res.cols_header[:-1])
            return df
            # Commit the transaction
            conn.commit()
            return {"res": "succ"}
        except Exception as e:
            catchException(e, conn)
            return {"res": "fail"}
        if conn is not None:
            conn.close()


    @app.route('/run_all')
    def run_all():
        f_time = time.time()
        print("start!")
        dropTables()
        createCsvTables()
        createMapfileTables()
        copyFromCsvToTable()
        copyFromMapfileToTable()
        s_time = time.time()
        joinCsvAndMapf()
        t_time = time.time()
        df = getTableAsDF()
        print(df)
        l_time = time.time()
        print("successful finished! at time: ")
        print(l_time - f_time)
        print("copy csv and mapfile to tables: ")
        print(s_time - f_time)
        print("join: ")
        print(t_time - s_time)
        print("create DF: ")
        print(l_time - t_time)
        return {"res": "succ"}

    if __name__ == '__main__':
        app.run(debug=True)

