import os

from numpy.core import long


def find_labels(path):
    try:
        labels = []
        with open(path + "\\labels.txt") as file:
            for i, l in enumerate(file):
                if i > 0:
                    start_date, start_time, end_date, end_time, tranportation_mode = l.split()
                    label_dict = {
                        "start_date_time": start_date.replace("/", "-") + " " + start_time,
                        "end_date_time": end_date.replace("/", "-") + " " + end_time,
                        "transportation_mode": tranportation_mode,
                    }
                    labels.append(label_dict)
        return True, labels
    except Exception:
        return False, []


class MyDataReader:

    def read(self):
        users = []
        activities = []
        trackpoints = []
        for dirname, dirnames, _ in os.walk('.\\dataset\\dataset\\Data'):
            for subdirname in dirnames:
                if subdirname != "Trajectory":
                    print("UserID:" + subdirname)
                    has_labels, labels = find_labels(os.path.join(dirname, subdirname))

                    complete_subdir = os.path.join(dirname, subdirname) + "\\Trajectory"
                    for _, _, filenames in os.walk(complete_subdir):
                        for i, filename in enumerate(filenames):

                            j = 0
                            # Enumerates through the file ones to check number of lines
                            with open(os.path.join(complete_subdir, filename), "r") as file:
                                for j, l in enumerate(file):
                                    pass
                            # Reads data from file if it is not too long
                            if j < 2506:
                                with open(os.path.join(complete_subdir, filename), "r") as file:
                                    for j, l in enumerate(file):
                                        if j > 6:
                                            lat, lon, _, altitude, date_days, date, time = l.split(",")
                                            trackpoint_dict = {
                                                "activity_id": int(filename.split(".")[0]),
                                                "lat": lat,
                                                "lon": lon,
                                                "altitude": altitude,
                                                "date_days": date_days,
                                                "date_time": (date + " " + time).strip(),
                                            }
                                            trackpoints.append(trackpoint_dict)

                                activity_dict = {
                                    "id": int(filename.split(".")[0]),
                                    "user_id": subdirname,
                                    "transportation_mode": None,
                                    "start_date_time": None,
                                    "end_date_time": None,
                                }
                                activities.append(activity_dict)

                    user_dict = {
                        "id": subdirname,
                        "has_labels": has_labels,
                    }
                    users.append(user_dict)
        print(len(activities))
        print(len(trackpoints))
        return users, activities, trackpoints


if __name__ == "__main__":
    MyDataReader()
