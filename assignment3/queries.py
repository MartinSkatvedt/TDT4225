from datetime import timedelta
from pprint import pprint 
from DbConnector import DbConnector
from haversine import haversine, Unit

class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

    
    def task_1(self):
        """
        How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database).
        """
        collection = self.db['User']
        print("Users:", collection.count_documents({}))
        collection = self.db['Activity']
        print("Activities:", collection.count_documents({}))
        collection = self.db['Trackpoint']
        print("Trackpoints:", collection.count_documents({}))

    def task_2(self):
        """
        Find the average number of activities per user.
        """
        collection = self.db['User']
        n_users = collection.count_documents({})
        collection = self.db['Activity']
        n_activities = collection.count_documents({})
        print("Average number of activities per user:", round(n_activities/n_users, 2))

    def task_3(self):
        """
        Find the top 20 users with the highest number of activities.
        """

        user_activities = {}

        collection = self.db['User']
        users = collection.find({})

        for user in users:
            count = len(user['activities'])
            user_activities[user['_id']] = count

        sorted_activities = sorted(user_activities.items(), key=lambda x: x[1], reverse=True)
        print("Top 20 users with the highest number of activities:")

        for i in range(20):
            print(sorted_activities[i])
    
    def task_4(self):
        """
        Find all users who have taken a taxi.
        """
        collection = self.db['Activity']
        users = collection.find({'transportation_mode': 'taxi'})

        users_set = set()
        for user in users:
            users_set.add(user['_id'][:3])
        
        print("Users who have taken a taxi:")
        print(sorted(users_set))

    def task_5(self):
        """
        Find all types of transportation modes and count how many activities that are tagged with 
        these transportation mode labels. Do not count the rows where the mode is null.
        """

        activities_dict = {}

        collection = self.db['Activity']
        activities = collection.find({'transportation_mode': {'$ne': None}})

        for activity in activities:
            mode = activity['transportation_mode']
            activities_dict.get(mode, 0)
            activities_dict[mode] = activities_dict.get(mode, 0) + 1
        
        print("Type of transportation mode : number of activities")
        pprint(activities_dict)

    def task_6_a(self):
        """
        Find the year with the most activities.
        """
        collection = self.db['Activity']
        year = collection.aggregate([
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'count': -1}
            },
            {
                '$limit': 1
            }
        ])

        print("Year with the most activities:")
        for doc in year:
            print(doc['_id'], doc['count'])

    def task_6_b(self):
        """
        Is this also the year with the most recorded hours?
        """
        collection = self.db['Activity']
        year = collection.aggregate([
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},
                    'count': {'$sum': {'$subtract': ['$end_date_time', '$start_date_time']}}
                }
            },
            {
                '$sort': {'count': -1}
            },
            {
                '$limit': 1
            }
        ])

        print("Year : number of hours")
        for doc in year:
            print(doc["_id"], round(doc["count"] / 1000 / 60 / 60, 2))
        
    def task_7(self):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """    

        collection = self.db['Activity']
        activities = collection.find({'_id': {'$regex': '^112.*'}, 'transportation_mode': 'walk', "$expr": {"$eq": [{ "$year": "$start_date_time" }, 2008]}})
        total_distance = 0
        collection = self.db['Trackpoint']
        for activity in activities:
            print(activity['_id'], activity['transportation_mode'], activity['start_date_time'])
            trackpoints = collection.find({'activity_id': activity['_id']})

            for index, trackpoint in enumerate(trackpoints):
                if index == 0:
                    prev_trackpoint = trackpoint
                    continue
                d = haversine((prev_trackpoint["lat"], prev_trackpoint["lon"]),  (trackpoint["lat"], trackpoint["lon"]))
                total_distance += d
                prev_trackpoint = trackpoint

        print("Total distance walked in 2008 by user 112:", round(total_distance, 2), "km")

    def task_8(self):
        """
        Find the top 20 users who have gained the most altitude meters
        """
        collection = self.db['Trackpoint']
        res = collection.aggregate([
            {
                "$group": {
                    "_id": "$activity_id",
                    "trackpoints": {"$push": "$altitude"},
                }
            }
        ])
        users = dict()
        
        for activity in res:
            for index, trackpoint in enumerate(activity["trackpoints"]):
                if index == 0:
                    prev_trackpoint = trackpoint
                    continue
                if trackpoint == -666: 
                    continue
                if (trackpoint - prev_trackpoint) > 0:
                    prev_count = users.get(activity["_id"][:3], 0)
                    users[activity["_id"][:3]] = prev_count + (trackpoint - prev_trackpoint)
                prev_trackpoint = trackpoint
                
        #Print in a pretty way
        #Sort 20 first based on number of altitude meters
        users = sorted(users.items(), key=lambda x: x[1], reverse=True)[:20]
        for user in users:
            print(user)
            
    def task_9(self):   
        """
        Find all users who have invalid activities, and the number of invalid activities per user
        """
        collection = self.db['Trackpoint']
        res = collection.aggregate([
            {
                "$group": {
                    "_id": "$activity_id",
                    "trackpoints": {"$push": "$date_time"},
                }
            }
        ])
        users = dict() # user_id : number of invalid activities
        
        for activity in res:
            for index, trackpoint in enumerate(activity["trackpoints"]):
                if index == 0:
                    prev_trackpoint = trackpoint
                    continue
                if (trackpoint - prev_trackpoint) > timedelta(minutes=5): # 5 minutes in milliseconds
                    prev_count = users.get(activity["_id"][:3], 0)
                    users[activity["_id"][:3]] = prev_count + 1
                    break
                prev_trackpoint = trackpoint
                
        #Print in a pretty way
        for user in users:
            print(user, users[user])


    def task_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing. 
        """
        # Radius perhaps?
        collection = self.db['Trackpoint']
        trackpoints = collection.find({'lat': {'$gte': 39.922, '$lt': 39.9226}, 'lon': {'$gte': 116.392, '$lt': 116.401}})
        
        users = set()
        for trackpoint in trackpoints:
            users.add(trackpoint['activity_id'][:3])

        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        print(sorted(users))

    def task_11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.
        """
        collection = self.db['Activity']
        res = collection.aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            {
                "$lookup": {
                    "from": "User",
                    "localField": "_id",
                    "foreignField": "activities",
                    "as": "user"
                }
            },
            {
                "$group": {
                    "_id": {
                        "user_id" : {"$arrayElemAt": ["$user._id", 0]},
                        "transportation_mode": "$transportation_mode"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
               "$sort": {"count": -1}
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "transportation_mode": {"$first": "$_id.transportation_mode"},
                    "count": {"$first": "$count"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ])

        # Print prettier 
        for doc in res: 
            print(doc['_id'], doc['transportation_mode'])




def main():
    program = None
    try:
        program = Queries()
        program.task_8()
  
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()