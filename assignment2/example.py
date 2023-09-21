from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

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
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
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
                activity_id INT,
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude INT NOT NULL,
                date_days DOUBLE NOT NULL,
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

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
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
        #program.create_user_table()
        #program.create_activity_table()
        #program.create_trackpoint_table()

        #program.insert_users()


        #program.show_tables()
        
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
