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
                   id VARCHAR(255) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_table_activity(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(255),
                   transportation_mode VARCHAR(255),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
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
        #program.create_table(table_name="Person")
        #program.insert_data(table_name="Person")
        #_ = program.fetch_data(table_name="Person")
        #program.drop_table(table_name="Person")

        program.create_table_user()
        program.create_table_activity()
        program.create_table_trackpoint()

        _ = program.fetch_data(table_name="User")
        _ = program.fetch_data(table_name="Activity")
        _ = program.fetch_data(table_name="TrackPoint")

        csv_data = csv.reader(open('dataset/Data/000/Trajectory/20081103101336.plt'))
        for i in range(6):
            next(csv_data)

        for row in csv_data:
            program.insert_data_trackpoint(row)
            print(row)

        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")

        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

    #for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
        #print(root)
        # print(dirs)
        # print(files)
        # print('--------------------------------')
