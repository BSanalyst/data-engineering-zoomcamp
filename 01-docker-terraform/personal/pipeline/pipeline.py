########################### Part 1 ############################
#We'll get some input and produce some output. Basic ETL
import sys

# will need to process some parameters for the pipeline
print("arguments", sys.argv)

month = int(sys.argv[1])
year = int(sys.argv[2])


print("hello pipeline","month:", month, "year:", year)

########################### Part 2: Pandas############################
import pandas as pd
df = pd.DataFrame({'day':[1,2,3], 'passenger':[4,5,6]})

# add month and year columns to the dataframe from the input parameters
df['month'] = month
df['year'] = year

print(df)

########################### Part 3: Save to Parquet ############################
df.to_parquet(f'output_passengers_{year}_{month}.parquet')

