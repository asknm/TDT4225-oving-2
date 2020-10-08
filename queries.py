# from MyDataReader import MyDataReader
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

    def trackpoint_table(self, table_name):
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
    def count_all(self, table_name):
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
        print("Average activities for each user:")
        print(count)

    # 4
    def taxi_users(self):
        query = """SELECT * FROM Activity WHERE Activity.id;"""  # hvis id finnes i labeled_ids.txt, hvordan er dette lagt inn i tabellen?

    # 5
    def all_transportations(self):
        query = """SELECT transportation_mode, count(transportation_mode) FROM activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Transportation modes and counts:")
        print(res)

    # 6a)
    def most_active_year_by_activity_count(self):
        query = """SELECT YEAR(start_date_time) as year
                FROM activity
                GROUP BY year
                ORDER BY COUNT(id) DESC
                LIMIT 1;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Most active year by activity count:")
        print(res)

    # 6b)
    def most_active_year_by_hours(self):
        query = """SELECT YEAR(start_date_time) as year, SUM(HOUR(TIMEDIFF(start_date_time, end_date_time))) AS hours
                FROM activity
                GROUP BY year
                ORDER BY hours DESC;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Most active year by hours logged:")
        print(res)

    # 9
    def find_invalid_activities(self):
        query = """SELECT user_id, COUNT(activity_id)
                FROM (activity as a JOIN 
                (SELECT DISTINCT tp1.activity_id
                FROM trackpoint AS tp1 JOIN trackpoint AS tp2 ON tp1.activity_id = tp2.activity_id 
                WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5 AND (tp1.id + 1) = tp2.id)
                AS invalids ON a.id = invalids.activity_id)
                GROUP BY a.user_id
                ORDER BY COUNT(activity_id) DESC;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Users with invalid activities:")
        print(res)

    # 10
    def activities_in_forbidden_city(self):
        query = """SELECT DISTINCT user.id 
                FROM user 
                INNER JOIN activity on activity.user_id=user.id 
                INNER JOIN trackpoint on trackpoint.activity_id=activity.id
                WHERE ROUND(trackpoint.lat, 2)=39.92 AND ROUND(trackpoint.lon, 2)=116.40;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Users that have activities in The Forbidden City:")
        print(res)


def main():
    program = None
    try:
        program = Program()
        program.activities_in_forbidden_city()

        # data_reader = myDataReader();
        # users, activities, trackpoints = data_reader.read()
        # program.count_all(table_name="User")
        # program.count_all(table_name="Activity")
        # program.count_all(table_name="TrackPoint")
        # program.user_table(table_name="User")
        # program.activity_table(table_name="Activity")
        # program.trackpoint_table(table_name="TrackPoint")
        # program.insert_data(table_name="User")
        # _ = program.fetch_data(table_name="User")
        # program.drop_table(table_name="User")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="TrackPoint")
        # Check that the table is dropped
        # program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
