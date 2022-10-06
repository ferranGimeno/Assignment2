from typing import re

from wheel.wheelfile import read_csv

from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
from datetime import timedelta

import geopy.distance
import os
import csv

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   name VARCHAR(30))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_table_user(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()
        self.set_autoincrement()

    def set_autoincrement(self):
        query = "ALTER TABLE User AUTO_INCREMENT = 0"
        self.cursor.execute(query)
        self.db_connection.commit()
        query = "TRUNCATE TABLE User"
        self.cursor.execute(query)
        self.db_connection.commit()
    def create_table_activity(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id INT,
                   transportation_mode VARCHAR(255) DEFAULT NULL,
                   start_date_time VARCHAR(255) DEFAULT NULL,
                   end_date_time VARCHAR(255) DEFAULT NULL,
                   FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_table_trackpoint(self):
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT,
                   lat DOUBLE,
                   lon DOUBLE,
                   val INT,
                   altitude INT,
                   date_days DOUBLE,
                   date_time VARCHAR(255),
                   FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def insert_data_user(self):
        labeled_list = []
        labeled_list_str = []
        csv_data = csv.reader(open('dataset/labeled_ids.txt'))
        for row in csv_data:
            num = int(row[0])
            labeled_list.append(num)
            labeled_list_str.append(row[0])

        for i in range(0, 182):
            if i in labeled_list:
                query = "INSERT INTO User (has_labels) VALUES (true)"
            else:
                query = "INSERT INTO User (has_labels) VALUES (false)"
            self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data_activity_test(self, user_id, row, found):
        user_id = int(user_id) + 1
        if found == 1:
            query = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES (" + str(user_id) + ", %s, %s, %s)"
            self.cursor.execute(query, row)
        else:
            query = "INSERT INTO Activity (user_id) VALUES (" + str(user_id) + ")"
            self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data_trackpoint_test(self, row):
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, val, altitude, date_days, date_time) VALUES " + row
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def insert_data(self):
        activity_id = 1
        data = []

        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                current_file = os.path.join(root, file)
                f = current_file.split("\\")

                # Not a Trajectory directory
                if "Trajectory" in f:
                    print(current_file)
                    csv_file = open(current_file)
                    n_lines = len(list(csv_file)) - 6
                    if n_lines <= 2500:
                        filename = current_file.replace("\\", "/").split("/")
                        start_time = filename[4]
                        start_time = start_time[:4] + "/" + start_time[4:]
                        start_time = start_time[:7] + '/' + start_time[7:]
                        start_time = start_time[:10] + ' ' + start_time[10:]
                        start_time = start_time[:13] + ':' + start_time[13:]
                        start_time = start_time[:16] + ':' + start_time[16:]
                        start_time = start_time.replace(".plt", "")
                        with open(current_file, "r") as trackpoints:
                            for line in trackpoints:
                                pass
                        last_line = line.split(",")
                        end_time = last_line[5] + " " + last_line[6]
                        end_time = end_time.replace("\n", "").replace("-", "/")
                        user_id = get_user(os.path.join(root))
                        found = 0
                        try:
                            line = check_labels(root, start_time, end_time)
                            if line != -1:
                                with open(os.path.join(root).replace("Trajectory", "labels.txt"), "r") as csvfile:
                                    csv_data = csv.reader(csvfile, delimiter='\t')
                                    index = 1
                                    for row in csv_data:
                                        if index == line and found != 1:
                                            found = 1
                                            data = row
                                        index = index + 1
                        except Exception:
                            pass

                        self.insert_data_activity_test(user_id, data, found)
                        with open(os.path.join(root, file), "r") as csvfile:
                            csv_data = csv.reader(csvfile, delimiter=',')
                            for i in range(0, 6):
                                csvfile.readline()
                            data = ""
                            for row in csv_data:
                                data = data + "(" + str(activity_id) + ", " + row[0] + ", " + row[1] + ", " + row[
                                    2] + ", " + row[3] + ", " + row[4] + ", " + row[5] + "),"
                            data = data[:-1]
                            self.insert_data_trackpoint_test(data)
                        activity_id = activity_id + 1

    def query_1(self):
        print("Query 1...")

        query_1_1 = "SELECT COUNT(*) FROM User"
        self.cursor.execute(query_1_1)
        rows_1_1 = self.cursor.fetchall()
        print(tabulate(rows_1_1, headers=self.cursor.column_names))

        query_1_2 = "SELECT COUNT(*) FROM Activity"
        self.cursor.execute(query_1_2)
        rows_1_2 = self.cursor.fetchall()
        print(tabulate(rows_1_2, headers=self.cursor.column_names))

        query_1_3 = "SELECT COUNT(*) FROM TrackPoint"
        self.cursor.execute(query_1_3)
        rows_1_3 = self.cursor.fetchall()
        print(tabulate(rows_1_3, headers=self.cursor.column_names))

        print("\n")

    def query_2(self):
        print("Query 2...")

        query_2 = "WITH Number AS (SELECT user_id, count(user_id) AS num_acts FROM Activity GROUP BY user_id) SELECT AVG(num_acts) FROM Number"
        self.cursor.execute(query_2)
        rows_2 = self.cursor.fetchall()
        print(tabulate(rows_2, headers=self.cursor.column_names))


        print("\n")

    def query_3(self):
        print("Query 3...")

        query_3 = "SELECT user_id, count(user_id) FROM Activity GROUP BY user_id ORDER BY count(user_id) DESC LIMIT 20"
        self.cursor.execute(query_3)
        rows_3 = self.cursor.fetchall()
        print(tabulate(rows_3, headers=self.cursor.column_names))


        print("\n")

    def query_4(self):
        print("Query 4...")

        query_4 = "SELECT user_id FROM Activity WHERE transportation_mode = 'taxi' GROUP BY user_id"
        self.cursor.execute(query_4)
        rows_4 = self.cursor.fetchall()
        print(tabulate(rows_4, headers=self.cursor.column_names))


        print("\n")

    def query_5(self):
        print("Query 5...")

        query_5 = "SELECT transportation_mode, count(*) FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode"
        self.cursor.execute(query_5)
        rows_5 = self.cursor.fetchall()
        print(tabulate(rows_5, headers=self.cursor.column_names))


        print("\n")

    def query_6a(self):
        print("Query 6a...")

        query_6a = "WITH Years AS (SELECT YEAR(start_date_time), count(id) AS num FROM Activity WHERE YEAR(start_date_time) IS NOT NULL GROUP BY YEAR(start_date_time)) SELECT * FROM Years ORDER BY num DESC LIMIT 1"
        self.cursor.execute(query_6a)
        rows_6a = self.cursor.fetchall()
        print(tabulate(rows_6a, headers=self.cursor.column_names))


        print("\n")

    def query_6b(self):
        diff_2007 = timedelta(seconds=0)
        diff_2008 = timedelta(seconds=0)
        diff_2009 = timedelta(seconds=0)
        diff_2011 = timedelta(seconds=0)
        print("Query 6b...")

        query_6b = "SELECT YEAR(start_date_time), TIME(start_date_time), TIME(end_date_time) FROM Activity WHERE start_date_time IS NOT NULL ORDER BY YEAR(start_date_time)"
        self.cursor.execute(query_6b)
        rows_6b = self.cursor.fetchall()
        for i in range(len(rows_6b)):
            if rows_6b[i][0] == 2007:
                start_time = datetime.strptime(str(rows_6b[i][1]), "%H:%M:%S")
                end_time = datetime.strptime(str(rows_6b[i][2]), "%H:%M:%S")
                diff_2007 += abs(end_time - start_time)

            if rows_6b[i][0] == 2008:
                start_time = datetime.strptime(str(rows_6b[i][1]), "%H:%M:%S")
                end_time = datetime.strptime(str(rows_6b[i][2]), "%H:%M:%S")
                diff_2008 += abs(end_time - start_time)

            if rows_6b[i][0] == 2009:
                start_time = datetime.strptime(str(rows_6b[i][1]), "%H:%M:%S")
                end_time = datetime.strptime(str(rows_6b[i][2]), "%H:%M:%S")
                diff_2009 += abs(end_time - start_time)

            if rows_6b[i][0] == 2011:
                start_time = datetime.strptime(str(rows_6b[i][1]), "%H:%M:%S")
                end_time = datetime.strptime(str(rows_6b[i][2]), "%H:%M:%S")
                diff_2011 += abs(end_time - start_time)

        print("Hours in 2007:", diff_2007)
        print("Hours in 2008:", diff_2008)
        print("Hours in 2009:", diff_2009)
        print("Hours in 2011:",diff_2011, "\n")

        if diff_2008 > diff_2007 and diff_2008 > diff_2009 and diff_2008 > diff_2011:
            print("Yes, 2008 is the year with the most recorded hours")
        else:
            print("No, 2008 is not the year with the most recorded hours")


        print("\n")

    def query_7(self):
        print("Query 7...")
        distance_km = 0.00
        query_7 = "SELECT * FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE user_id = '113' AND transportation_mode = 'walk' AND YEAR(start_date_time) = '2008'"
        self.cursor.execute(query_7)
        rows_7 = self.cursor.fetchall()

        for i in range(len(rows_7)):
            if i+1 < len(rows_7):
                coords_1 = (rows_7[i][2], rows_7[i][3])
                coords_2 = (rows_7[i+1][2], rows_7[i+1][3])
                distance_km += geopy.distance.geodesic(coords_1, coords_2).km
        print("The total distance walked by the user with id = 112 in 2008 is:", ("%.2f" % distance_km), "km")

        print("\n")


    def query_8(self):
        print("Query 8...")
        gained_altitude_feet = 0
        users_gained_altitude = []
        query_8 = "SELECT user_id, altitude FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id"
        self.cursor.execute(query_8)
        rows_8 = self.cursor.fetchall()
        for j in range(1,183):
            for i in range(len(rows_8)):
                if i+1 < len(rows_8) and rows_8[i][0] == j and rows_8[i+1][0] == j:
                    start_altitude = rows_8[i][1]
                    end_altitude = rows_8[i+1][1]
                    gained_altitude_feet += end_altitude - start_altitude
            print("user:", j, ",gained altitude:", gained_altitude_feet, "feet")
            users_gained_altitude.append([gained_altitude_feet,j])
            gained_altitude_feet = 0
        top_20_users_gained_altitude = sorted(users_gained_altitude)[len(users_gained_altitude)-20:len(users_gained_altitude)]

        print("\n")
        print("Top 20 users:\n")
        for i in range(len(top_20_users_gained_altitude)):
            print("User:",top_20_users_gained_altitude[i][1], "gained altitude:", ("%.2f" % (top_20_users_gained_altitude[i][0]*0.3048)), "meters")
        print("\n")

    def query_9(self):
        print("Query 9...")
        invalid_trackpoints = []
        invalid_activities = []
        query_9 = "WITH Dif AS (SELECT user_id, activity_id, date_days, LEAD(date_days) OVER(ORDER BY date_days) as next_date FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id) SELECT user_id, count(DISTINCT activity_id) FROM Dif WHERE next_date - date_days > 0.03472 and activity_id IS NOT NULL GROUP BY user_id ORDER BY user_id"
        self.cursor.execute(query_9)
        rows_9 = self.cursor.fetchall()
        print(tabulate(rows_9, headers=self.cursor.column_names))


        print("\n")

    def query_10(self):
        print("Query 10...")

        query_10 = "SELECT user_id FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE SUBSTR(CAST(lat AS char),1,6) = '39.916' AND SUBSTR(CAST(lon AS char),1,7) = '116.397' GROUP BY user_id"
        self.cursor.execute(query_10)
        rows_10 = self.cursor.fetchall()
        print(tabulate(rows_10, headers=self.cursor.column_names))


        print("\n")

    def query_11(self):
        print("Query 11..")

        query_11 = "WITH Top AS (SELECT user_id, transportation_mode, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(transportation_mode) DESC) rn FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode) SELECT user_id, transportation_mode FROM Top WHERE rn = 1"
        self.cursor.execute(query_11)
        rows_11 = self.cursor.fetchall()
        print(tabulate(rows_11, headers=self.cursor.column_names))


        print("\n")


def check_labels(root, start_time, end_time):
    index = 1
    with open(os.path.join(root).replace("Trajectory", "labels.txt"), "r") as labels:
        for label in labels:
            if str(start_time) in label and end_time in label:
                #print("label:", label)
                #print("Start:", start_time, "End:", end_time)
                return index
            index = index + 1
        return -1

def get_user(param):
    user = param.replace("\\", "/").split("/")
    return user[2]

def main():
    program = None
    try:
        program = ExampleProgram()

        # program.drop_table(table_name="TrackPoint")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="User")
        #
        # program.create_table_user()
        # program.create_table_activity()
        # program.create_table_trackpoint()
        #
        # program.insert_data_user()
        # program.insert_data()

        # Check that the table is dropped
        # _ = program.fetch_data(table_name="User")
        # _ = program.fetch_data(table_name="Activity")
        # _ = program.fetch_data(table_name="TrackPoint")

        # program.query_1()
        # program.query_2()
        # program.query_3()
        # program.query_4()
        # program.query_5()
        # program.query_6a()
        # program.query_6b()
        # program.query_7()
        # program.query_8()
        program.query_9()
        # program.query_10()
        # program.query_11()

        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()


