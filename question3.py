import os
# QUESTION 3
def parse_files(dir, file_type):
    file_list = [x for x in os.listdir(dir) if x.endswith(file_type)]
    return file_list