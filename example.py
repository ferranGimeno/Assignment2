from typing import re

from wheel.wheelfile import read_csv

from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime

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

        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")

        program.create_table_user()
        program.create_table_activity()
        program.create_table_trackpoint()

        program.insert_data_user()
        program.insert_data()

        # Check that the table is dropped
        #_ = program.fetch_data(table_name="User")
        #_ = program.fetch_data(table_name="Activity")
        #_ = program.fetch_data(table_name="TrackPoint")

        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()


