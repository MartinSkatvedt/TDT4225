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


    def task_1(self):
        """
        How many users, activities and points are there in the 
        dataset (after it is inserted into the database).  
        """

        query = """
        WITH UserCount AS (
            SELECT COUNT(*) AS user_count FROM User
        ),
        ActivityCount AS (
            SELECT COUNT(*) AS activity_count FROM Activity
        ),
        TrackpointCount AS (
            SELECT COUNT(*) AS trackpoint_count FROM Trackpoint
        )
        SELECT
            user_count,
            activity_count,
            trackpoint_count
        FROM UserCount, ActivityCount, TrackpointCount

        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print(tabulate(result, headers=["User count", "Activity count", "Trackpoint count"]))

    def task_2(self):
        """
        Find the average, maximum and minimum number of trackpoints per user.
        """
        query = """
        WITH TrackpointsPerUser AS (
            SELECT 
                user_id,
                Activity.id,
                COUNT(*) as n_trackpoints
            FROM Activity
            JOIN Trackpoint ON Activity.id = Trackpoint.activity_id
            GROUP BY user_id, id
        ),
        RankTrackpointsPerUserDesc AS (
            SELECT 
                user_id, 
                n_trackpoints,
                RANK() OVER (PARTITION BY user_id ORDER BY n_trackpoints DESC) AS TP_rank
            FROM TrackpointsPerUser
        ),
        MaxTrackpointsPerUser AS (
            SELECT 
                user_id, 
                n_trackpoints 
            FROM RankTrackpointsPerUserDesc
            WHERE TP_rank = 1
        ),
        RankTrackpointsPerUserAsc AS (
            SELECT
                user_id,
                n_trackpoints,
                RANK() OVER (PARTITION BY user_id ORDER BY n_trackpoints ASC) AS TP_rank
            FROM TrackpointsPerUser
            ORDER BY user_id
        ),
        MinTrackpointsPerUser AS (
            SELECT
                user_id,
                n_trackpoints
            FROM RankTrackpointsPerUserAsc
            WHERE TP_rank = 1
        ),
        AverageTrackpointsPerUser AS (
            SELECT
                user_id,
                AVG (n_trackpoints) OVER (PARTITION BY user_id) AS avg_trackpoints
            FROM TrackpointsPerUser
        )

        SELECT
            DISTINCT min_tp.user_id, min_tp.n_trackpoints, max_tp.n_trackpoints, avg_tp.avg_trackpoints
        FROM MinTrackpointsPerUser min_tp
        JOIN MaxTrackpointsPerUser max_tp ON min_tp.user_id = max_tp.user_id
        JOIN AverageTrackpointsPerUser avg_tp ON min_tp.user_id = avg_tp.user_id

        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print(tabulate(result[0:15], headers=["User ID", "Min trackpoints", "Max trackpoints", "Average trackpoints"]))
  
    def task_3(self):
        """
        Find the top 15 users with the highest number of activities. 
        """ 

        query = """
        SELECT 
            user_id,
            COUNT(*) AS activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 15
        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=["User ID", "Activity count"]))

    def task_4(self):
        """
        Find all users who have taken a bus. 
        """

        query = """
        SELECT 
            DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'bus'
        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=["User ID"]))

    def task_5(self):
        """
        List the top 10 users by their amount of different transportation modes.
        """
        
        query = """
        WITH TransportationModes AS (
            SELECT 
                user_id, 
                transportation_mode
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        )
        SELECT
            user_id,
            COUNT(*) AS n_transportation_modes
        FROM TransportationModes
        GROUP BY user_id
        ORDER BY n_transportation_modes DESC
        LIMIT 10
        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print(tabulate(result, headers=["User ID", "Number of transportation modes"]))

    def task_6(self):
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
        
    def task_7(self):
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
        print("Number of users with multiple day activities: " , len(set(userList)))

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
        
    def task_8(self):
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

    def task_9(self):
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

 
    def task_10(self):
        """
        Find users that have traveled the longest total distance in one
        day for each transportation mode
        """
        #Fetch all transportation modes
        query="SELECT DISTINCT transportation_mode FROM Activity WHERE transportation_mode IS NOT NULL AND DATEDIFF(Activity.end_date_time, Activity.start_date_time) <= 1"
        self.cursor.execute(query)
        transportation_modes = self.cursor.fetchall()
        distances = {}
        for transportation_mode in transportation_modes:
            query = "SELECT * FROM Activity WHERE transportation_mode='%s'"
            self.cursor.execute(query % transportation_mode[0])
            activities = self.cursor.fetchall()
            for activity in activities:
                #Get all trackpoints per activity 
                query = "SELECT lat, lon, date_time FROM Trackpoint where activity_id='%s' ORDER BY date_time ASC"
                self.cursor.execute(query % activity[0])
                trackpoints = self.cursor.fetchall()
                distance = 0
                for trackpoint in trackpoints:
                    # Calculate distance between current point and next point
                    distance += self.distance_between_two_points(trackpoints[trackpoints.index(trackpoint)-1][0], trackpoints[trackpoints.index(trackpoint)-1][1], trackpoint[0], trackpoint[1])

                    date = trackpoint[2].date()
                    #Check if distances is empty and add distance
                    if distances.get(transportation_mode[0]) == None:
                        distances.update({transportation_mode[0]: [date, distance, activity[1]]})
                    #Check if distance is longer than previous distance
                    elif distances.get(transportation_mode[0])[1] < distance:
                        distances.update({transportation_mode[0]: [date, distance, activity[1]]})
        print(tabulate([[k,] + v for k,v in distances.items()], headers=["Transportation Mode", "Date", "Distance", "User ID"]))
        

    def task_11(self):
        """
        Find all users who have invalid activities, 
        and the number of invalid activities per user.
        An invalid activity is defined as an activity with consecutive trackpoints where the timestamp deviate with at least 5 minutes.
        """
        query = """SELECT trackpoint1.activity_id
                           FROM Trackpoint AS trackpoint1
                           JOIN Trackpoint AS trackpoint2 ON trackpoint1.id + 1 = trackpoint2.id
                           WHERE trackpoint1.activity_id = trackpoint2.activity_id AND TIMESTAMPDIFF(MINUTE, trackpoint1.date_time, trackpoint2.date_time) >= 5
                           GROUP BY trackpoint1.activity_id"""
        # Fetch all activities with invalid trackpoints
        self.cursor.execute(query)
        invalid_activities = self.cursor.fetchall()
        print(invalid_activities)
        users = {} # user_id : number of invalid activities
        for activities in invalid_activities:
            # The first three characters of the activity_id is the user_id
            user_id = activities[0][0:3]
            if users.get(user_id) == None:
                users.update({user_id: 1})
            else:
                users.update({user_id: users.get(user_id) + 1})

        print(tabulate(users.items(), headers=["User ID", "Number of invalid activities"]))

    def task_12(self):
        """
        Find all users who have registered transportation_mode 
        and their most used transportation_mode.
        """

        query = """
        WITH TM_COUNTS AS (
            SELECT 
                user_id, 
                transportation_mode, 
                COUNT(*) as tm_count
            FROM Activity 
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ),
        Ranked_TM AS (
            SELECT 
                user_id, 
                transportation_mode, 
                tm_count,
                DENSE_RANK() OVER (PARTITION BY user_id ORDER BY tm_count DESC) AS tm_rank
            FROM TM_COUNTS
        )
        SELECT 
            user_id, 
            transportation_mode
        FROM Ranked_TM
        WHERE tm_rank = 1
        """

        self.cursor.execute(query)
        users_with_most_used_tm = self.cursor.fetchall()

        print(tabulate(users_with_most_used_tm[0:15], headers=["User ID", "Transportation mode"]))
    
def main():
    program = None
    try:
       program = Queries()
       program.task_7()
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()