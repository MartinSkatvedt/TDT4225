from collections import defaultdict
import datetime
import math
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
        query = "SELECT id, user_id, start_date_time, end_date_time FROM %s ORDER BY start_date_time ASC"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()

        overlapping_activities = [] 
        
        for index, row in enumerate(rows):
            current_user = row[1]
            current_start_date_time = row[2]
            current_end_date_time = row[3]

            for i in range(index + 1, len(rows)): 
                if current_user == rows[i][1]:
                    continue

                if current_end_date_time >= rows[i][3] and current_start_date_time <= rows[i][2]:
                    overlapping_activities.append([row, rows[i]])
                    break

        return overlapping_activities
    
    
    def distance_between_two_points(self, lat1, lon1, lat2, lon2):
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        lon1_rad = math.radians(lon1)
        lon2_rad = math.radians(lon2)

        # Haversine formula
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad

        a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
        c = 2 * math.asin(math.sqrt(a))

        r = 6371 # Radius of earth in kilometers
        distance = (c * r) * 1000 # Return distance in meters
        return distance
        


    def nUsersWhichHaveBeenClose(self):
        overlapping_activities = self.overlappingActivities()

        has_been_close = defaultdict(set)
        table_name = "Trackpoint"
        for index, activities in enumerate(overlapping_activities):
            print(f'{index + 1}/{len(overlapping_activities)}')

            #check if users have been close before
            meetups_user1 = has_been_close.get(activities[0][1], set())
            meetups_user2 = has_been_close.get(activities[1][1], set())
            if activities[1][1] in meetups_user1 or activities[0][1] in meetups_user2:
                continue
            #fetch trackpoints for first activity
            query = "SELECT lat, lon, date_time, id FROM %s WHERE activity_id = '%s'"
            self.cursor.execute(query % (table_name, activities[0][0]))
            trackpoints_user_1 = self.cursor.fetchall()

            #fetch trackpoints for second activity
            query = "SELECT lat, lon, date_time, id FROM %s WHERE activity_id = '%s'"
            self.cursor.execute(query % (table_name, activities[1][0]))
            trackpoints_user_2 = self.cursor.fetchall()

            #check if trackpoints are close in distance 
            for trackpoint_user_1 in trackpoints_user_1:
                for trackpoint_user_2 in trackpoints_user_2:
                    distance = self.distance_between_two_points(trackpoint_user_1[0], trackpoint_user_1[1], trackpoint_user_2[0], trackpoint_user_2[1])
                    if distance <= 50:
                        delta_t = trackpoint_user_1[2] - trackpoint_user_2[2]
                        delta_sec = abs(delta_t.total_seconds())
                        if delta_sec <= 30:
                            print(f'User: {activities[0][1]} and user: {activities[1][1]} have been close to each other', distance, delta_sec, trackpoint_user_1[3], trackpoint_user_2[3])
                            has_been_close[activities[0][1]].add(activities[1][1])
                            break
                else:
                    continue
                break
        unique_users = set()
        for key, value in has_been_close.items():
            unique_users.add(key)
            for user in value:
                unique_users.add(user)
        
        print(f'Number of unique users that have been close to each other: {len(unique_users)}')

    def gainedAltitude(self):
        altitudes_dict = {} # user_id : altitude

        #Fetches all users
        query = "SELECT id FROM %s"
        self.cursor.execute(query % ("User"))
        user_ids = self.cursor.fetchall()

        #Loops over every user
        for user_id in user_ids:
            print(user_id[0])

            #Queries all tracpoints associated with the user, using LIKE user_id%
            query = "SELECT altitude, activity_id FROM %s WHERE activity_id LIKE '%s' ORDER BY activity_id ASC"
            self.cursor.execute(query % ("Trackpoint", user_id[0] + "%"))
            altitudes = self.cursor.fetchall()
            if len(altitudes) == 0:
                continue

            #Iterates over all trackpoints and calculates the altitude gained
            # -666 is used to indicate that the trackpoint is invalid
            # The first trackpoint is used as starting value, and reset on a new activity
            altitude_n = altitudes[0][0]
            user_total = 0
            current_activity = altitudes[0][1]
            for i in range(1, len(altitudes)):
                if altitudes[i][1] != current_activity:
                    current_activity = altitudes[i][1]
                    altitude_n = altitudes[i][0]
                    continue
                if altitudes[i][0] == -666:
                    continue
                if altitudes[i][0] > altitude_n:
                    user_total += altitudes[i][0] - altitude_n
                altitude_n = altitudes[i][0]
            
            prev_total = altitudes_dict.get(user_id[0], 0)
            altitudes_dict.update({user_id[0]: prev_total + user_total})

        #Sort the dictionary and print the top 15
        sorted_dict = sorted(altitudes_dict.items(), key=lambda item: item[1], reverse=True)
        print(tabulate(sorted_dict[0:15], headers=["User ID", "Altitude gained"]))

def main():
    program = None
    try:
       program = Queries()
       program.gainedAltitude()
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()