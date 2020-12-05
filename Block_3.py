#Defining imports
import pandas as pd

exec(open("./question1.py").read())
exec(open("./question2.py").read())
exec(open("./question3.py").read())
exec(open("./question4.py").read())
exec(open("./question5.py").read())

# UNIT TESTS
if __name__ == "__main__":
    print("Question 1 ...")
    assert [1, 2, 5, 8, 10] == sort_numerical_list(["5", "8", "1", "2", "10"]), "Inorrect sorting"
    print("... passed")

    print("Question 2 ...")
    input_file = "D:/paack/Data/drivers_table.csv"
    assert 1384 == count_capital_letters(input_file), "Wrong Counting"
    print("... passed")

    print("Question 3 ...")
    target_folder = "D:/paack/Data/"
    expected_result = ['drivers_summary.csv', 'drivers_table.csv', 'orders_table.csv']
    assert expected_result == parse_files(target_folder, ".csv"), "Inorrect sorting"
    print("... passed")

    print("Question 4 ...")
    df = compute_drivers_performance()
    assert 0.25 == df[df.id == 2].Performance.values, "Inorrect sorting"
    assert ['block3_out.csv'] == parse_files("D:/paack/Data/Out/", ".csv"), "Inorrect sorting"
    print("... passed")

    print("Question 5 ...")
    config = '''
    user: MY_USER
    password: MY_PWD
    account: account.eu-central-1
    warehouse: MY_WAREHOUSE
    database: MY_DATABASE
    schema: MY_SCHEMA
    '''
    config_yaml = yaml.load(config)
    df = etl_example(config_yaml)
    assert '1951' == df.Any.min(), "Inorrect sorting"
    print("... passed")