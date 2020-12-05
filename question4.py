#Defining imports
import pandas as pd

# QUESTION 4
def compute_drivers_performance():
    #pandas library will be used.
    #First we import both tables needed for the exercise from the csv provided
    orders_df = pd.read_csv("D:/paack/Data/orders_table.csv", sep=',')
    drivers_df = pd.read_csv("D:/paack/Data/drivers_table.csv", sep=',')

    #Before continuing we filter out duplicates
    orders_df.drop_duplicates(keep='first', inplace=True)
    drivers_df.drop_duplicates(keep='first', inplace=True) #Not really needed in this particular case

    #now we convert the time data into datetime type
    orders_df['Delivery Start'] = pd.to_datetime(orders_df['Delivery Start'])
    orders_df['Delivery End'] = pd.to_datetime(orders_df['Delivery End'])
    orders_df['Attempted time'] = pd.to_datetime(orders_df['Attempted time'])

    #lets filter out if there are deliveries with non valid attempt times (not the case)
    orders_df = orders_df[ ~orders_df.isnull() ]

    #lets compute the perfromance of each driver
    aggr_df = orders_df.groupby(['driver_id', 'Deliver date']) \
        .agg( {'Delivery Start':'min', 'Delivery End': 'max', 'id': lambda x: x.nunique()}) \
        .rename(columns={'id':'orders'}) \
        .reset_index(drop=False) 
    aggr_df['WorkHours'] = aggr_df['Delivery End'].apply(lambda x: x.hour) - aggr_df['Delivery Start'].apply(lambda x: x.hour)
    aggr_df['Performance'] = aggr_df.orders / aggr_df.WorkHours

    #Finally obtain the full river list with its performance. Drivers that did not work will be accounted for NULL performance
    merged_df = pd.merge(drivers_df, aggr_df, left_on='id', right_on='driver_id', how='left')
    merged_df.drop(columns={'driver_id'}, inplace=True)

    merged_df.to_csv("D:/paack/Data/Out/block3_out.csv", sep=',')
    return merged_df