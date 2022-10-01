from wheel.wheelfile import read_csv

from DbConnector import DbConnector
from tabulate import tabulate

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

    def create_table_activity(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id INT,
                   transportation_mode VARCHAR(255),
                   start_date_time VARCHAR(255),
                   end_date_time VARCHAR(255),
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
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
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

    def insert_data_trackpoint(self, row):
        query = "INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(query, row)
        self.db_connection.commit()

    def insert_data_user(self):
        labeled_list = []
        labeled_list_str = []
        csv_data = csv.reader(open('dataset/labeled_ids.txt'))
        for row in csv_data:
            num = int(row[0])
            labeled_list.append(num)
            labeled_list_str.append(row[0])

        for i in range(1, 181):
            if i in labeled_list:
                query = "INSERT INTO User (has_labels) VALUES (true)"
            else:
                query = "INSERT INTO User (has_labels) VALUES (false)"
            self.cursor.execute(query)
        self.db_connection.commit()
        return labeled_list_str

    def discard_lines(self, file):
        file.readline()
        file.readline()
        file.readline()
        file.readline()
        file.readline()
        file.readline()
    def insert_data_activity(self, row, user_id):
        query = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES (" + user_id + ", %s, %s, %s)"
        self.cursor.execute(query, row)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
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


        labeled_list_str = program.insert_data_user()
        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")
                print(currentFile)
                if f[1] in labeled_list_str:
                    print(f[1])
                    csv_file = open(currentFile)
                    nLines = len(list(csv_file)) + 1
                    if nLines <= 2500:
                        #If it's Trajectory is not in the path it means it's a labels.txt
                        if not "Trajectory" in currentFile:
                            with open(currentFile) as csvfile:
                                csvfile.readline()
                                csv_data = csv.reader(csvfile, delimiter='\t')
                                for row in csv_data:
                                    print(row)
                                    program.insert_data_activity(row, f[1])
                        #If there is Trajectory in the path we want to save the .plt
                        else:
                            with open(currentFile) as csvfile:
                                program.discard_lines(csvfile)
                                csv_data = csv.reader(csvfile, delimiter=',')
                                for row in csv_data:
                                    print(row)
                                    program.insert_data_trackpoint(row)

        """for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for dir in dirs:
                if dir in labeled_list_str:
                    print(dir)
                    file = open(root + "/" + dir + "/labels.txt")
                    nLines = len(list(file)) + 1
                    if nLines <= 2500:
                        with open(root + "/" + dir + "/labels.txt") as csvfile:
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter='\t')
                            #for row in csv_data:
                                #print(row)
                                #program.insert_data_activity(row, dir)
                            print("--------------------------")"""


        """for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for dir in dirs:
                if dir in labeled_list_str:
                    for (root2, dirs2, files2) in os.walk(root+"/"+dir+"/Trajectory/", topdown=True):
                        for file2 in files2:
                            with open(root2+"/"+file2) as csvfile:
                                row_count = 0
                                for row in csvfile:
                                    row_count += 1
                                if row_count <= 2500:
                                    print(root2 + "/" + file2, row_count)"""

        # Check that the table is dropped
        _ = program.fetch_data(table_name="User")
        _ = program.fetch_data(table_name="Activity")
        _ = program.fetch_data(table_name="TrackPoint")

        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()


