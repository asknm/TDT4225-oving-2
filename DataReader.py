import os


class DataReader:

    def __init__(self):
        for dirname, dirnames, filenames in os.walk('.\dataset\dataset\Data'):
            # print path to all subdirectories first.
            for subdirname in dirnames:
                print(os.path.join(dirname, subdirname))
            #
            # # print path to all filenames.
            # for filename in filenames:
            #     print(os.path.join(dirname, filename))

        # file = open("to_visit.txt", "r")


if __name__ == "__main__":
    DataReader()
