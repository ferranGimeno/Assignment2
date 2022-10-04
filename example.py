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

    def insert_data_trackpoint(self, row):
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
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

    def insert_data_activity_test(self, user_id, row):
        query = "INSERT INTO Activity (user_id, start_date_time, end_date_time, transportation_mode) VALUES (" + user_id + ", %s, %s, %s)"
        #query = "BULK INSERT " + file + " FROM 'C:\Demos\sample-csv-file-for-demo.csv' WITH (FIELDTERMINATOR =' ', ROWTERMINATOR ='\n');"
        print(query)
        self.cursor.execute(query, row)
        self.db_connection.commit()

    def insert_data_trackpoint_test(self, activity_id, option, row):
        if option == 0:
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES ("+str(activity_id)+","+row[0]+","+row[1]+","+row[2]+","+row[3]+","+row[4]+")"
        else:
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (" + str(activity_id) + ", null, 0, 0, 0, null)"
        #query = "BULK INSERT " + file + " FROM 'C:\Demos\sample-csv-file-for-demo.csv' WITH (FIELDTERMINATOR =' ', ROWTERMINATOR ='\n');"
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_act_id_into_trackpoint(self):
        query = "INSERT INTO TrackPoint(activity_id) (SELECT Activity.id FROM Activity, TrackPoint WHERE date_time > start_date_time AND date_time < end_date_time)"
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

def check_labels(root, start_time, end_time):
    index = 1
    with open(os.path.join(root).replace("Trajectory", "labels.txt"), "r") as labels:
        for label in labels:
            if str(start_time) in label and end_time in label:
                print("label:", label)
                print("Start:", start_time, "End:", end_time)
                return index
            index = index + 1
        return -1

def get_user(param):
    user = param.replace("\\", "/").split("/")
    return user[2]

def check_lines(param, margin):
    pass

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
        activity_id = 1
        default = ["null", "null", "null"]

        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                # Debugging pourposes
                if file in "dataset/Data/010/Trajectory/20080926104408.plt":
                    program.fetch_data("Activity")
                    program.fetch_data("TrackPoint")
                    return
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")
                # Not a Trajectory directory
                if "Trajectory" in f:
                    csv_file = open(currentFile)
                    nLines = len(list(csv_file)) - 6
                    if nLines <= 2500:
                        filename = currentFile.replace("\\", "/").split("/")
                        start_time = filename[4]
                        start_time = start_time[:4] + "/" +start_time[4:]
                        start_time = start_time[:7] + '/' + start_time[7:]
                        start_time = start_time[:10] + ' ' + start_time[10:]
                        start_time = start_time[:13] + ':' + start_time[13:]
                        start_time = start_time[:16] + ':' + start_time[16:]
                        start_time = start_time.replace(".plt", "")
                        with open(currentFile, "r") as trackpoints:
                            for line in trackpoints:
                                pass
                        last_line = line.split(",")
                        end_time = last_line[5] + " " + last_line[6]
                        end_time = end_time.replace("\n", "").replace("-", "/")
                        user_id = get_user(os.path.join(root))
                        print(user_id)
                        try:
                            line = check_labels(root, start_time, end_time)
                            if line != -1:
                                with open(os.path.join(root).replace("Trajectory", "labels.txt"), "r") as csvfile:
                                    csv_data = csv.reader(csvfile, delimiter='\t')
                                    index = 1
                                    for row in csv_data:
                                        if index == line:
                                            program.insert_data_activity_test(user_id, row)
                                        index = index + 1
                            else:
                                print("ERROR")
                                program.insert_data_activity_test(user_id, default)
                        except Exception:
                            #program.insert_data_activity_test(user_id, default)
                            pass

                        with open(currentFile) as csvfile:
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter=',')
                            for row in csv_data:
                                user = get_user(currentFile)
                                if user in labeled_list_str:
                                    #program.insert_data_trackpoint(row)
                                    #program.insert_data_trackpoint_test(activity_id, 0)
                                    pass
                                else:
                                    pass
                                    #program.insert_data_trackpoint_test(activity_id, 1)
                        activity_id = activity_id + 1

        return

        for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                #Debugging pourposes
                if file in "dataset/Data/010/Trajectory/20080926104408.plt":
                    return
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")
                print(currentFile)
                #Not a Trajectory directory
                if not "Trajectory" in f:
                    with open(currentFile) as csvfile:
                        csvfile.readline()
                        csv_data = csv.reader(csvfile, delimiter='\t')
                        for row in csv_data:
                            print(row)
                            start_time = row[0].replace('/', '').replace(':', '').replace(' ', '') + ".plt"
                            try:
                                with open(os.path.join(root + "/Trajectory/", start_time).replace("\\", "/"), 'r') as f:
                                    for line in f:
                                        pass
                                    last_line = line
                                print(start_time)
                                path = os.path.join(root + "/Trajectory/", start_time)
                                last = last_line.split(",")
                                end_time = last[5] + " " + last[6]
                                end_time = end_time.replace("\n", "").replace("-", "/")
                                print("End:", end_time, ", Row:", row[1], "#")
                                if end_time == row[1]:
                                    print("Found")
                                    found_plts.append(path)
                            except Exception:
                                pass

                            #Abrir fichero .plt con el nombre de start_time
                            #Si coincide, abrir y si existe, mirar la ultima linea
                            #Si coincide, se reabre el fichero y se guarda primero la Activity y se guardan los TrakPoint
                            #print(row)
                            #program.insert_data_activity(row, f[1])
                #Thats a Trajectory directory
                else:
                    if currentFile not in found_plts:
                        print(currentFile)
                    """program.insert_data_activity_test(f[1])
                    csv_file = open(currentFile)
                    nLines = len(list(csv_file)) - 6
                    print(nLines)
                    if nLines <= 2500:
                        print("Reading")
                        with open(currentFile) as csvfile:
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter=',')
                            for row in csv_data:
                                #print(row)
                                #print(activity_id)
                                #program.insert_data_trackpoint(row)
                                program.insert_data_trackpoint_test(activity_id)
                    activity_id = activity_id + 1"""

        """for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for file in files:
                currentFile = os.path.join(root, file)
                f = currentFile.split("\\")
                #print(currentFile)
                if f[1] in labeled_list_str:
                    print(f[1])
                    #Not a Trajectory directory
                    if not "Trajectory" in f:
                        with open(currentFile) as csvfile:
                            csvfile.readline()
                            csv_data = csv.reader(csvfile, delimiter='\t')
                            for row in csv_data:
                                print(row)
                                start_time = row[0].replace("/", "")
                                start_time = start_time.replace(" ", )
                                #Abrir fichero .plt con el nombre de start_time
                                #Si coincide, abrir y si existe, mirar la ultima linea
                                #Si coincide, se reabre el fichero y se guarda primero la Activity y se guardan los TrakPoint
                                #print(row)
                                #program.insert_data_activity(row, f[1])
                    #Thats a Trajectory directory
                    else:
                        program.insert_data_activity_test(f[1])
                        csv_file = open(currentFile)
                        nLines = len(list(csv_file)) - 6
                        print(nLines)
                        if nLines <= 2500:
                            print("Reading")
                            with open(currentFile) as csvfile:
                                csvfile.readline()
                                csvfile.readline()
                                csvfile.readline()
                                csvfile.readline()
                                csvfile.readline()
                                csvfile.readline()
                                csv_data = csv.reader(csvfile, delimiter=',')
                                for row in csv_data:
                                    #print(row)
                                    #print(activity_id)
                                    #program.insert_data_trackpoint(row)
                                    program.insert_data_trackpoint_test(activity_id)
                        activity_id = activity_id + 1"""

        """for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
            for dir in dirs:
                if dir in labeled_list_str:
                    root_labels = root+"/"+dir+"/labels.txt"
                    with open(root_labels) as csvfile:
                        csvfile.readline()
                        csv_data = csv.reader(csvfile, delimiter='\t')
                        for row in csv_data:
                            print(row)
                            program.insert_data_activity(row, dir)"""

        """activity = program.fetch_data_2(table_name="Activity")
        possible_plts = program.show_possible_plts(activity)
        for i in range(1):
            dir = labeled_list_str[i]
            real_plts = program.show_real_plts(possible_plts, dir)
            for j in range(len(real_plts)):
                root_plt = "dataset/Data/"+dir+"/Trajectory/" + real_plts[j][0]
                csv_file = open(root_plt)
                nLines = len(list(csv_file)) + 1
                if nLines <= 700:
                    print(root_plt)
                    with open(root_plt) as csvfile:
                        program.discard_lines(csvfile)
                        csv_data = csv.reader(csvfile, delimiter=',')
                        for row in csv_data:
                            date_time = row[5] + " " + row[6]
                            data = [real_plts[j][1], row[0], row[1], row[3], row[4], date_time]
                            if row[3] != -777:
                                program.insert_data_trackpoint(data)"""


        """for (root, dirs, files) in os.walk('dataset/Data', topdown=True):
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
                                        program.insert_data_trackpoint(data)""

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


