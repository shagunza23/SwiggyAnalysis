# importing libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, FloatType
from paths import path_for_input_csv, path_for_output_dir
from utils import create_dimension_tables, create_csv


def main(schema, spark):

    # Load the main DataFrame
    df = spark.read.csv(path_for_input_csv, header=True, schema=schema)

    # Calling create_dimension function to make dimension DataFrames
    rest_dim = create_dimension_tables(df, "rest_id", ["restaurant_name"])
    city_dim = create_dimension_tables(df, "city_id", ["city"])
    locality_dim = create_dimension_tables(df, "locality_id", ["locality"])
    cuisines_dim = create_dimension_tables(df, "cuisine_id", ["cuisines"])
    rating_star_dim = create_dimension_tables(df, "rating_id", ["rating_stars", "rating_text"])
    table_booking_dim = create_dimension_tables(df, "table_booking_id", ["has_table_booking"])
    delivery_avail_dim = create_dimension_tables(df, "deliver_id", ["has_online_delivery"])


# Steps to create fact table
    mid_df = df.join(rest_dim, df["restaurant_name"] == rest_dim["restaurant_name"], "inner") \
        .join(city_dim, df["city"] == city_dim["city"], "inner") \
        .join(locality_dim, df["locality"] == locality_dim["locality"], "inner") \
        .join(cuisines_dim, df["cuisines"] == cuisines_dim["cuisines"], "inner") \
        .join(table_booking_dim, df["has_table_booking"] == table_booking_dim["has_table_booking"], "inner") \
        .join(delivery_avail_dim, df["has_online_delivery"] == delivery_avail_dim["has_online_delivery"], "inner") \
        .join(rating_star_dim, df["rating_stars"] == rating_star_dim["rating_stars"], "inner")

    swiggy_fact = mid_df.select(["rest_id", "city_id", "locality_id", "cuisine_id", "table_booking_id", "deliver_id",
                                 "rating_id", "average_cost_for_two", "price_range", "votes"])

    swiggy_fact = swiggy_fact.withColumn("Fact_id", monotonically_increasing_id() + 1)

    final_df = (
        swiggy_fact.alias("fact")
        .join(rest_dim.alias("rest"), col("fact.rest_id") == col("rest.rest_id"), "inner")
        .join(city_dim.alias("city"), col("fact.city_id") == col("city.city_id"), "inner")
    )

    # swiggy_fact.show()

    # Optionally select specific columns
    df_count_restaurant_city_wise = final_df.select(
        col("rest.rest_id"),
        col("city")
    )

    # Report 1
    '''Query for counting restaurant city wise'''
    count_restaurant_city_wise = spark.sql(
        '''Select city,count(rest_id) as count from {df} group by city''',
        df=df_count_restaurant_city_wise)

    # count_restaurant_city_wise.show(truncate=False)
    create_csv(count_restaurant_city_wise, path_for_output_dir, r"\report1.csv")

    # Report 2
    '''Query for City-wise Top 10 restaurant based on Avg cost for 2'''
    df_top10_restaurant_avg_cost2 = final_df.select(
        col("restaurant_name"),
        col("city"),
        col("average_cost_for_two")
    )

    top10_restaurant_avg_cost2 = spark.sql(
        '''select city, restaurant_name as Restaurant_Name, average_cost_for_two as Average_Cost_for_two 
                    from (select city, restaurant_name, average_cost_for_two,
                    row_number() over(partition by city order by average_cost_for_two desc) as rankk 
                    from {df} order by average_cost_for_two desc)
                    where rankk<=10 order by city''', df=df_top10_restaurant_avg_cost2
    )
    # top10_restaurant_avg_cost2.show()
    create_csv(top10_restaurant_avg_cost2, path_for_output_dir, r"\report2.csv")

    # Report 3
    '''Top 10 restaurant based on Avg Votes'''
    votes_df = final_df.select(["city", "restaurant_name", "votes"])

    votes_top10_citywise = spark.sql(
        '''select city, restaurant_name as Restaurant_Name, votes as Votes from
         (select city, restaurant_name, votes,
          row_number() over(partition by city order by votes desc) as rankk from {df}
          order by votes desc)
          where rankk<=10''', df=votes_df
    )

    # votes_top10_citywise.show(truncate=False)
    create_csv(votes_top10_citywise, path_for_output_dir, r"\report3.csv")

    # Report 4
    '''Top 10 restaurant based on Rating City wise'''
    rating_df = final_df.join(rating_star_dim.alias("rating"), col("fact.rating_id") == col("rating.rating_id"),
                              "inner")

    '''dividing query into 3 parts for better understanding'''
    df_part1 = spark.sql(
        '''
         select restaurant_name,city, max(rating_stars) as rating_stars from {df} 
         group by city, restaurant_name  order by rating_stars desc ''',
        df=rating_df
    )

    df_part2 = spark.sql(
        ''' select restaurant_name,city,rating_stars,
            row_number() over (partition by city order by rating_stars desc) as rankk from {df} 
            order by rating_stars desc''',
        df=df_part1)

    df_top10_restaurant_rating_wise = spark.sql(
        '''select restaurant_name,city,rating_stars from {df} where rankk<=10''', df=df_part2
    )

    # df_top10_restaurant_rating_wise.show(truncate=False)
    create_csv(df_top10_restaurant_rating_wise, path_for_output_dir, r"\report4.csv")

    # Report 5
    '''Rating based on delivery availability City wise'''
    df_delivery_avail_city_wise = (rating_df.join(delivery_avail_dim.alias("delivery"),
                                   col("fact.deliver_id") == col("delivery.deliver_id"), "inner")
                                   .select(["city", "has_online_delivery", "rating_stars"]))

    delivery_avail_city_wise = spark.sql(
        '''select city,has_online_delivery as Online_Delivery, round(avg(rating_stars),1) as Rating_Stars from
            {df} group by city, has_online_delivery order by city,Online_Delivery desc''',
        df=df_delivery_avail_city_wise
    )
    # delivery_avail_city_wise.show()
    create_csv(delivery_avail_city_wise, path_for_output_dir, r"\report5.csv")

    # Report 6
    '''Avg cost for 2 based on cuisine city wise'''

    df_cuisine_wise_avg_cost = (final_df.join(cuisines_dim.alias("cuisine"),
                                col("fact.cuisine_id") == col("cuisine.cuisine_id"), "inner")
                                .select(["city", "cuisines", "average_cost_for_two"]))

    cuisine_wise_avg_cost = spark.sql(
        '''select city,cuisines as Cuisine, round(avg(average_cost_for_two),1) as Average_Cost_for_two from
        {df} group by city, cuisines order by city''', df=df_cuisine_wise_avg_cost
    )

    # cuisine_wise_avg_cost.show(truncate=False)
    create_csv(cuisine_wise_avg_cost, path_for_output_dir, r"\report6.csv")


# Initializing Spark session
spark = SparkSession.builder.appName("Swiggy Fact Analysis").getOrCreate()
spark.sparkContext.setLogLevel("OFF")


# Define the schema
restaurant_schema = StructType([
    StructField("restaurant_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("cuisines", StringType(), True),
    StructField("average_cost_for_two", IntegerType(), True),
    StructField("has_table_booking", StringType(), True),
    StructField("has_online_delivery", StringType(), True),
    StructField("rating_stars", FloatType(), True),
    StructField("rating_text", StringType(), True),
    StructField("price_range", IntegerType(), True),
    StructField("votes", IntegerType(), True)
])


if __name__ == "__main__":
    main(restaurant_schema, spark)
