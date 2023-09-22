import os
import datetime


def parse_plt_file(filename):
    data = [] #[lat, lon, altitude, date, time]
    with open(filename, 'r') as f:
        lines = f.readlines()
        lines = lines[6:] #Skipping first six lines

        if len(lines) > 2500:
            return []

        for line in lines:
            split_data = line.strip().split(',')
            lat = split_data[0]
            lon = split_data[1]
            altitude = split_data[3]
            date = split_data[5]
            time = split_data[6]

            data.append([lat, lon, altitude, date, time])
    
    return data
    
def parse_labeled_file(filename):
    labels = [] #[start_date, start_time, end_date, end_time, label]
    with open(filename, 'r') as f:
        lines = f.readlines()
        lines = lines[1:]

        for line in lines:
            split_data = line.strip().split('\t')
            start_datetime = split_data[0]
            start_date = start_datetime.split(' ')[0]
            start_time = start_datetime.split(' ')[1]
            end_datetime = split_data[1]
            end_date = end_datetime.split(' ')[0]
            end_time = end_datetime.split(' ')[1]
            label = split_data[2]

            labels.append([start_date, start_time, end_date, end_time, label])

    return labels

# Create use dict 
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

for i in range(0, 182):
    name_str = str(i)
    if i < 10:
        name_str = "00" + name_str
    elif i < 100:
        name_str = "0" + name_str


    print(f'Cleaning data for user {name_str}...')

    dirname="./dataset/Data/" + name_str + "/Trajectory/"
    cleaned_dirname="./dataset/CleanedData/" + name_str
    os.makedirs(cleaned_dirname, exist_ok=True)
    trajectory_files=os.listdir(dirname)

    if users[name_str]:
        labeled_filename = "./dataset/Data/" + name_str + "/labels.txt"
        parsed_labels = parse_labeled_file(labeled_filename)

        for index, filename in enumerate(trajectory_files):
            if filename.endswith(".plt"):
                parsed = parse_plt_file(dirname + filename)
                if parsed == []:
                    continue
                start_date = parsed[0][3]
                start_time = parsed[0][4]
                end_date = parsed[-1][3]
                end_time = parsed[-1][4]

                activity_start_datetime = datetime.datetime.strptime(start_date + " " + start_time, "%Y-%m-%d %H:%M:%S")
                activity_end_datetime = datetime.datetime.strptime(end_date + " " + end_time, "%Y-%m-%d %H:%M:%S")


                activity_labels = []
                for parsed_label in parsed_labels:
                    label_start_datetime = datetime.datetime.strptime(parsed_label[0] + " " + parsed_label[1], "%Y/%m/%d %H:%M:%S")
                    label_end_datetime = datetime.datetime.strptime(parsed_label[2] + " " + parsed_label[3], "%Y/%m/%d %H:%M:%S")

                    if activity_start_datetime.timestamp() == label_start_datetime.timestamp() and activity_end_datetime.timestamp() == label_end_datetime.timestamp():
                        activity_labels.append(parsed_label[4])


                
                label_string = ""
                if len(activity_labels) == 0:
                    label_string = "undefined"
                elif len(activity_labels) == 1:
                    label_string = activity_labels[0]
                else:
                    label_string = "/".join(activity_labels) 
                txt_filename = str(index) + ".txt"
                with open(cleaned_dirname + "/" + txt_filename, "w") as f:
                    f.write(name_str + "," + start_date + " " + start_time + "," + end_date + " " + end_time + "," + label_string + "\n")
                    f.write("lat,lon,altitude,date,time\n")
                    for line in parsed:
                        f.write(line[0] + "," + line[1] + "," + line[2] + "," + line[3] + "," + line[4] + "\n") 
    else:
        for index, filename in enumerate(trajectory_files):
            if filename.endswith(".plt"):
                parsed = parse_plt_file(dirname + filename)
                if parsed == []:
                    continue
                start_date = parsed[0][3]
                start_time = parsed[0][4]
                end_date = parsed[-1][3]
                end_time = parsed[-1][4]

                txt_filename = str(index) + ".txt"
                with open(cleaned_dirname + "/" + txt_filename, "w") as f:
                    f.write(name_str + "," + start_date + " " + start_time + "," + end_date + " " + end_time + ", undefined" + "\n")
                    f.write("lat,lon,altitude,date,time\n")
                    for line in parsed:
                        f.write(line[0] + "," + line[1] + "," + line[2] + "," + line[3] + "," + line[4] + "\n")
    

