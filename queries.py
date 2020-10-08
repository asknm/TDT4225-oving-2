from MyDataReader import MyDataReader
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit


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
    # 1
    def count_all(self,table_name):
        query = """ SELECT COUNT(*) FROM %s;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Count of " % table_name)
        print(count)


    # 2
    def agerage_activities(self):
        query = """ SELECT (SELECT COUNT(*) FROM activity)/(SELECT COUNT(*) FROM user);"""
        # query = """ SELECT COUNT(activity.id)/COUNT(user.id) FROM activity, user;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Average activities for each user:")
        print(count)

    # 3
    def top_twenty_users(self):
        query = """SELECT user_id FROM activity GROUP BY user_id ORDER BY COUNT(*) DESC limit 20;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Top twenty users:")
        print(count)

    # 4
    def taxi_users(self):
        query = """SELECT user.id FROM activity WHERE transportation_mode = 'taxi' GROUP BY user.id;""" # hvis id finnes i labeled_ids.txt, hvordan er dette lagt inn i tabellen?
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Top taxi users:")
        print(count)


    # 5
    def all_transportations(self):
        query = """
        SELECT transportation_mode, count(transportation_mode)
        FROM activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Transportation modes and counts:")
        print(res)

    # 6
    def most_active_year(self):
        query = ""

    # 7
    def user_112_distance_walked_2008(self):
        query = """
        SELECT t.activity_id, t.lat, t.lon, t.altitude
        FROM trackpoint AS t
        INNER JOIN activity AS a ON t.activity_id=a.id
        WHERE YEAR(a.start_date_time) = 2008
        AND a.user_id = '112'
        AND a.transportation_mode = 'walk'
        ORDER BY t.activity_id;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        distance = 0.0
        for i, l in enumerate(res):
            if i > 0 and l[0] == res[i-1][0]:
                distance += haversine((l[1], l[2]), (res[i-1][1], res[i-1][2]))
        print("Total distance:")
        print(distance)


    # 11
    def transportation_mode_users(self):


def main():
    try:
        program = Program()
        program.user_112_distance_walked_2008()

        # data_reader = myDataReader();
        # users, activities, trackpoints = data_reader.read()
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
