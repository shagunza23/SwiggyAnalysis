# Importing Libraries
from pyspark.sql.functions import monotonically_increasing_id
import os


# Function to create the dimension tables
def create_dimension_tables(df, id_column, select_columns):
    # Select distinct entries from the specified columns
    df_new = df.select(select_columns).distinct()
    df_new = df_new.withColumn(id_column, monotonically_increasing_id()+1)

    # df_new.show(truncate=False)  # to be removed while saving the output

    return df_new


def create_csv(df, direc, file_name):
    try:
        if not os.path.exists(direc):
            os.makedirs(direc)

        df.toPandas().to_csv(direc+file_name, index=False)

    except Exception as e:
        print(e)
