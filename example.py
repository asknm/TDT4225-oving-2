from MyDataReader import MyDataReader
from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(50) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id BIGINT UNSIGNED NOT NULL,
                user_id VARCHAR(50) NOT NULL references user(id),
                transportation_mode VARCHAR(20),
                start_date_time DATETIME,
                end_date_time DATETIME,
                PRIMARY KEY (id, user_id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id BIGINT UNSIGNED NOT NULL references activity(id),
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_user_data(self, data):
        query = "INSERT INTO user VALUES (%(id)s, %(has_labels)s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def insert_activity_data(self, data):
        query = """INSERT INTO activity VALUES (%(id)s, %(user_id)s, %(transportation_mode)s, 
                %(start_date_time)s, %(end_date_time)s)
                """
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def insert_trackpoint_data(self, data):
        query = """INSERT INTO trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%(activity_id)s, %(lat)s, %(lon)s,
                %(altitude)s, %(date_days)s, %(date_time)s)
        """
        self.cursor.executemany(query, data)
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
        query = "DROP TABLE IF EXISTS %s "
        self.cursor.execute(query % table_name)
        query = """ALTER TABLE %s
                        ADD """
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()


def main():
    # program = None
    # data_reader = MyDataReader()
    # users, activities, trackpoints = data_reader.read()
    # print(activities)
    try:
        program = ExampleProgram()
        program.drop_table(table_name="user")
        program.drop_table(table_name="activity")
        program.drop_table(table_name="trackpoint")
        program.create_user_table(table_name="user")
        program.create_activity_table(table_name="activity")
        program.create_trackpoint_table(table_name="trackpoint")
        data_reader = MyDataReader()
        users, activities, trackpoints = data_reader.read()
        print(activities)
        program.insert_user_data(users)
        program.insert_activity_data(activities)
        # program.insert_trackpoint_data(trackpoints)
        for i in range(20000, len(trackpoints), 20000):
            print(str(100*i/len(trackpoints)) + " %")
            program.insert_trackpoint_data(trackpoints[i-20000:i])
        program.insert_trackpoint_data(trackpoints[i:])
        _ = program.fetch_data(table_name="user")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
