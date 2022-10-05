from wheel.wheelfile import read_csv

from DbConnector import DbConnector
from tabulate import tabulate
import pandas

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
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        self.cursor.execute(query, row)
        self.db_connection.commit()
    #     (SELECT Activity.id FROM Activity order by id desc limit 1),
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


    def insert_act_id_into_trackpoint(self):
        query = "INSERT INTO TrackPoint(activity_id) (SELECT Activity.id FROM Activity, TrackPoint WHERE date_time > start_date_time AND date_time < end_date_time)"
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_data_2(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def show_possible_plts(self, table):
        possible_plts = []
        for i in range(len(table)):
            if table[i][3] is not None:
                plt_name = table[i][3][0:4] + table[i][3][5:7] + table[i][3][8:10] + table[i][3][11:13] + \
                           table[i][3][14:16] + table[i][3][17:19] + ".plt"
                possible_plts.append([plt_name, i+1])

        return possible_plts

    def show_real_plts(self, possible_plts, dir):
        real_plts = []
        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")

                if f[1] == dir:
                    for i in range(len(possible_plts)):
                        if file in possible_plts[i][0]:
                            real_plts.append([file, i+1])


        return real_plts

    def insert_labeled_data_activity_tuple(self, tuple):
        str_tuple = str(tuple)
        str_tuple_1 = str_tuple[1:len(str_tuple)-1]
        str_tuple_2 = str_tuple[1:len(str_tuple)-2]
        if str_tuple[len(str_tuple_1)] == ")":
            query = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES " + str_tuple_1
        elif str_tuple[len(str_tuple_1)] == ",":
            query = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES " + str_tuple_2
        # print(query)
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_not_labeled_data_activity_tuple(self):
        query = "INSERT INTO Activity (user_id) (SELECT id FROM User WHERE has_labels = 0)"
        # print(query)
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_data_trackpoint_tuple(self, tuple):
        str_tuple = str(tuple)
        str_tuple_1 = str_tuple[1:len(str_tuple)-1]
        str_tuple_2 = str_tuple[1:len(str_tuple)-2]
        if str_tuple[len(str_tuple_1)] == ")":
            query = "INSERT INTO TrackPoint (lat,lon,altitude,date_days,date_time) VALUES " + str_tuple_1
        elif str_tuple[len(str_tuple_1)] == ",":
            query = "INSERT INTO TrackPoint (lat,lon,altitude,date_days,date_time) VALUES " + str_tuple_2
        # print(query)
        self.cursor.execute(query)
        self.db_connection.commit()

    def update_trackpoint_tuple(self):
        query = "UPDATE TrackPoint, Activity SET TrackPoint.activity_id = Activity.id WHERE TrackPoint.date_time < Activity.end_date_time and TrackPoint.date_time > Activity.start_date_time and TrackPoint.activity_id"
        # print(query)
        self.cursor.execute(query)
        self.db_connection.commit()

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
            for dir in dirs:
                if "Trajectory" not in dir:
                    if dir in labeled_list_str:
                        root_labels = root+"/"+ dir +"/labels.txt"
                        with open(root_labels) as csvfile:
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter='\t')
                            csv_list = [(dir,) + tuple(line) for line in csv_data]
                            csv_tuple = tuple(csv_list)
                            program.insert_labeled_data_activity_tuple(csv_tuple)

        program.insert_not_labeled_data_activity_tuple()



        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                if "Trajectory" in root and "010" in root:
                    csv_file = open(root + "/" + file)
                    nLines = len(list(csv_file)) + 1
                    if nLines <= 2500:
                        with open(root + "/" + file) as fil:
                            print(root + "/" + file)
                            program.discard_lines(fil)
                            csv_data = csv.reader(fil, delimiter=',')
                            csv_list = [tuple([line[0], line[1], line[3], line[4],line[5]+" "+line[6]]) for line in csv_data]
                            csv_tuple = tuple(csv_list)
                            program.insert_data_trackpoint_tuple(csv_tuple)
        program.update_trackpoint_tuple()





        # activity = program.fetch_data_2(table_name="Activity")
        # possible_plts = program.show_possible_plts(activity)
        # for i in range(1):
        #     dir = labeled_list_str[i]
        #     real_plts = program.show_real_plts(possible_plts, dir)
        #     for j in range(len(real_plts)):
        #         root_plt = "dataset/Data/"+dir+"/Trajectory/" + real_plts[j][0]
        #         csv_file = open(root_plt)
        #         nLines = len(list(csv_file)) + 1
        #         if nLines <= 700:
        #             print(root_plt)
        #             with open(root_plt) as csvfile:
        #                 program.discard_lines(csvfile)
        #                 csv_data = csv.reader(csvfile, delimiter=',')
        #                 for row in csv_data:
        #                     date_time = row[5] + " " + row[6]
        #                     data = [real_plts[j][1], row[0], row[1], row[3], row[4], date_time]
        #                     if row[3] != -777:
        #                         program.insert_data_trackpoint(data)




        """
        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")
                print(currentFile)
                if f[1] in labeled_list_str:
                    # print(f[1])
                    if not "Trajectory" in currentFile:
                        print(currentFile)
                        with open(currentFile) as csvfile:
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter='\t')
                            for row in csv_data:
                                # print(row)
                                program.insert_data_activity(row, f[1])
                    
                    else:
                        csv_file = open(currentFile)
                        nLines = len(list(csv_file)) + 1
                        if nLines <= 2500:
                            with open(currentFile) as csvfile:
                                program.discard_lines(csvfile)
                                csv_data = csv.reader(csvfile, delimiter=',')
                                for row in csv_data:
                                    date_time = row[5] + " " + row[6]
                                    data = [row[0], row[1], row[3], row[4], date_time]
                                    if row[3] != -777:
                                        program.insert_data_trackpoint(data)
                                        
        """

                    #     #If there is Trajectory in the path we want to save the .plt
                    #     else:
                    #         with open(currentFile) as csvfile:
                    #             program.discard_lines(csvfile)
                    #             csv_data = csv.reader(csvfile, delimiter=',')
                    #             for row in csv_data:
                    #                 date_time = row[5] + " " + row[6]
                    #                 data =  [row[0], row[1], row[3], row[4], date_time]
                    #                 if row[3] != -777:
                    #                     program.insert_data_trackpoint(data)
                        # program.insert_act_id_into_trackpoint()



        # for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
        #     for dir in dirs:
        #         if dir in labeled_list_str:
        #             print(dir)
        #             file = open(root + "/" + dir + "/labels.txt")
        #             nLines = len(list(file)) + 1
        #             if nLines <= 2500:
        #                 with open(root + "/" + dir + "/labels.txt") as csvfile:
        #                     csvfile.readline()
        #                     csv_data = csv.reader(csvfile, delimiter='\t')
        #                     #for row in csv_data:
        #                         #print(row)
        #                         #program.insert_data_activity(row, dir)
        #                     print("--------------------------")
        #
        #
        # for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
        #     for dir in dirs:
        #         if dir in labeled_list_str:
        #             for (root2, dirs2, files2) in os.walk(root+"/"+dir+"/Trajectory/", topdown=True):
        #                 for file2 in files2:
        #                     with open(root2+"/"+file2) as csvfile:
        #                         row_count = 0
        #                         for row in csvfile:
        #                             row_count += 1
        #                         if row_count <= 2500:
        #                             print(root2 + "/" + file2, row_count)


        # Check that the table is dropped
        # _ = program.fetch_data(table_name="User")
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


