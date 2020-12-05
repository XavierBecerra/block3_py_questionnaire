# QUESTION 1
def sort_numerical_list(list):
    # The given input is a list of numeric strings
    # The desired behaviour is to order the numbers ascending.
    # First convert them to integer type, otherwise the sorting will place the string '10' before '2', but we want 2 before 10
    test_list = [int(i) for i in list] 
    # then we sort the list using the implicit method sort(key=, reverse= )
    test_list.sort()
    return test_list