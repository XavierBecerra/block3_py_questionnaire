# QUESTION 2
def count_capital_letters(input_file):
    #First we open the target file un utf-8 encosing
    with open(input_file, encoding="utf8") as file:
        counter = 0
        #then we iterate through all characters and if it is an upper letter we increase the counter by one
        for char in file.read():
            if char.isupper():
                counter += 1
    return counter