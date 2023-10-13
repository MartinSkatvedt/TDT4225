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
            
    
    def task_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing. 
        """

        collection = self.db['Trackpoint']
        trackpoints = collection.find({'lat': {'$gte': 39.916, '$lt': 39.917}, 'lon': {'$gte': 116.397, '$lt': 116.398}})
        
        users = set()
        for trackpoint in trackpoints:
            users.add(trackpoint['activity_id'][:3])

        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        print(sorted(users))



def main():
    program = None
    try:
        program = Queries()
        program.task_10()
  
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()