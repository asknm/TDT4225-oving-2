from MyDataReader import MyDataReader
from DbConnector import DbConnector
from tabulate import tabulate


class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                           id VARCHAR(4) AUTO_INCREMENT NOT NULL PRIMARY KEY,
                           has_labels TINYINT(1))
                        """
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                           id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                           user_id VARCHAR(4) NOT NULL FOREIGN KEY,
                           start_date_time DATETIME ,
                           end_date_time DATETIME,
                           PRIMARY KEY (id, user_id))
                        """
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def trackpoint_table(self,table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                           id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                           activity_id VARCHAR(4) NOT NULL FOREIGN KEY,
                           lat DOUBLE,
                           lon DOUBLE,
                           altitude INT,
                           date_days DOUBLE,
                           date_time DATETIME 
                        """
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
        query = """INSERT INTO trackpoint VALUES (%(activity_id)s, %(lat)s, %(lon)s
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

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    ### task 2
    def count_all(self,table_name):
        query = """ SELECT COUNT(*) FROM %s;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Count of " % table_name)
        print(count)


    def agerage_activities(self):
        query = """ SELECT COUNT(activity.id)/COUNT(user.id) FROM activity, user;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Average activities for each user:")
        print(count)

    def top_twenty_users(self):
        query = """SELECT user_id FROM activity GROUP BY user_id ORDER BY COUNT(*) DESC limit 20;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Average activities for each user:")
        print(count)

    def taxi_users(self):
        query = """SELECT * FROM Activity WHERE Activity.id;""" # hvis id finnes i labeled_ids.txt, hvordan er dette lagt inn i tabellen?


    def all_transportations(self):
        query = """SELECT * FROM User ORDER BY Activities DESC limit 20;""" # trenger labeled_ids.txt

    def most_active_year(self):
        query = """SELECT LEFT(..., 4) AS INT FROM activity ORDER BY ... COUNT(*) DESC limit 1;"""

def main():
    program = None
    try:
        program = Program()
        data_reader = myDataReader();
        users, activities, trackpoints = data_reader.read()
        # program.count_all(table_name="User")
        # program.count_all(table_name="Activity")
        # program.count_all(table_name="TrackPoint")
        #program.user_table(table_name="User")
        #program.activity_table(table_name="Activity")
        #program.trackpoint_table(table_name="TrackPoint")
        #program.insert_data(table_name="User")
        # _ = program.fetch_data(table_name="User")
        #program.drop_table(table_name="User")
        #program.drop_table(table_name="Activity")
        #program.drop_table(table_name="TrackPoint")
        # Check that the table is dropped
        #program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()