from collections import defaultdict
import datetime
from DbConnector import DbConnector
from tabulate import tabulate

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def averageMinAndMaxTrackpoints(self):
        count_dict = {} # user_id : n_trackpoints
        table_name = "Activity"
        query = "SELECT id, user_id FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        for row in rows:
            activity_id = row[0]
            user_id = row[1]
            query = "SELECT COUNT(*) FROM Trackpoint WHERE activity_id = '%s'"
            self.cursor.execute(query % activity_id)
            count = self.cursor.fetchall()
            prev_count = count_dict.get(user_id, 0)
            count_dict.update({user_id: prev_count + count[0][0]})

        min = ["000", float("inf")]
        max = ["000", float("-inf")]
        total = 0.0
        for user, count in count_dict.items():
            total += count
            if count < min[1]:
                min = [user, count]
            if count > max[1]:
                max = [user, count]
        average = total / len(count_dict)

        print("Max number of trackpoints: " + str(max[1]) + " for user: " + max[0])
        print("Min number of trackpoints: " + str(min[1]) + " for user: " + min[0])
        print(f'Average over {len(count_dict)} users is: {average}')

    def top15Users(self):
        activity_dict = {} # user_id : n_activities
        table_name = "Activity"
        query = "SELECT user_id FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        for row in rows:
            user_id = row[0]
            prev_activity = activity_dict.get(user_id, 0)
            activity_dict.update({user_id: prev_activity + 1})
            
        sorted_dict = sorted(activity_dict.items(), key=lambda item: item[1], reverse=True)
        print(sorted_dict[0: 15])
        
    def top10UsersWithUniqueActivities(self):
        activity_dict = defaultdict(set) # user_id : transportation_mode[]
        table_name = "Activity"
        query = "SELECT user_id, transportation_mode FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        for row in rows:
            if row[1]:
                user_id = row[0]
                transportation_mode = row[1]
                activity_dict[user_id].add(transportation_mode)
        activity_counts = []
        for key, value in activity_dict.items():
            activity_counts.append([key, len(value)])

        sorted_counts = sorted(activity_counts, key=lambda item: item[1], reverse=True)
        print(sorted_counts[0: 10])
            
    def multipleActivities(self):
        table_name = "Activity"
        query = "SELECT user_id, start_date_time, end_date_time FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        for row in rows:
            user_id = row[0]
            start_date_time = row[1]
            end_date_time = row[2]
            query = "SELECT COUNT(*) FROM Activity WHERE user_id = '%s' AND start_date_time = '%s' AND end_date_time = '%s'"
            self.cursor.execute(query % (user_id, start_date_time, end_date_time))
            count = self.cursor.fetchall()
            if count[0][0] > 1:
                print("User: " + user_id + " has multiple activities with the same start and end date time")
        
    def nUsersThatHasStartedAnActivityAndEndedItTheNextDay(self):
        #This answers both a) and b)
        table_name = "Activity"
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        multipleDayActivities = []
        userList = []
        for row in rows:
            start_date_time = row[3]
            end_date_time = row[4]
            start_date_time_delta = start_date_time + datetime.timedelta(days=1)
            if start_date_time_delta.day == end_date_time.day:
                multipleDayActivities.append((row[1], row[2], end_date_time - start_date_time))
                userList.append(row[1])

        for activity in multipleDayActivities: 
            print("User ID: " , activity[0] , " Transportation mode: " , activity[1] , " Duration: " , activity[2])

    def overlappingActivities(self):
        table_name = "Activity"
        query = "SELECT id, start_date_time, end_date_time FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        for row in rows:
            print(row[1])


def main():
    program = None
    try:
       program = Queries()
       program.overlappingActivities()
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()