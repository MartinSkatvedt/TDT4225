import os
from DbConnector import DbConnector


class InsertData:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN NOT NULL
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "User")
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES User(id)
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "Activity")
        self.db_connection.commit()

   
    def create_trackpoint_table(self):
        query = """ CREATE TABLE IF NOT EXISTS %s (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id VARCHAR(255),
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude INT NOT NULL,
                date_time DATETIME NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
                )
                """

        self.cursor.execute(query % "Trackpoint")
        self.db_connection.commit()
    
    def insert_users(self):
        users = {}

        for i in range(0, 182):
            name_str = str(i)
            if i < 10:
                name_str = "00" + name_str
            elif i < 100:
                name_str = "0" + name_str

            users.update({name_str: False})

        with open("dataset/labeled_ids.txt", "r") as f:
            read_users = f.readlines()

            for user in read_users:
                users.update({user.strip(): True})


        for user in users:
            query = "INSERT INTO %s (id, has_labels) VALUES ('%s', %s)"
            self.cursor.execute(query % ("User", user, users[user]))
        
        self.db_connection.commit()

    def insert_activities(self):
        for i in range(0, 182):
            user_id = str(i)
            if i < 10:
                user_id = "00" + user_id
            elif i < 100:
                user_id = "0" + user_id
            
            dirname="./dataset/CleanedData/" + user_id
            activity_files = os.listdir(dirname)

            for activity_file in activity_files:
                with open(dirname + "/" + activity_file, "r") as f:
                    activity_id = int(activity_file.replace(".txt", ""))
                    
                    if activity_id < 10: 
                        activity_id = "00" + str(activity_id)
                    
                    elif activity_id < 100:
                        activity_id = "0" + str(activity_id)

                    activity_id = user_id + str(activity_id)

                    data = f.readline()
                    data = data.strip().split(',')

                    query = ""
                    if data[3].strip() == "undefined":
                        query = "INSERT INTO %s (id, user_id, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                        self.cursor.execute(query % ("Activity",activity_id, user_id, data[1], data[2]))
                    else:
                        query = "INSERT INTO %s (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s', '%s')"
                        self.cursor.execute(query % ("Activity",activity_id, user_id, data[3], data[1], data[2]))
            self.db_connection.commit()

    def insert_trackpoints(self):
       for i in range(0, 182):
            user_id = str(i)
            if i < 10:
                user_id = "00" + user_id
            elif i < 100:
                user_id = "0" + user_id

            print(user_id)
            
            dirname="./dataset/CleanedData/" + user_id
            activity_files = os.listdir(dirname)

            for activity_file in activity_files:
                pathname = dirname + "/" + activity_file
                query="LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 2 LINES (activity_id, lat, lon, altitude, date_time);"

                self.cursor.execute(query % (pathname,"Trackpoint"))

            self.db_connection.commit()

def main():
    program = None
    try:
        program = InsertData()

        # Create tables
        program.create_user_table()
        program.create_activity_table()
        program.create_trackpoint_table()

        # Insert data
        program.insert_users()
        program.insert_activities()
        program.insert_trackpoints()
        
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()