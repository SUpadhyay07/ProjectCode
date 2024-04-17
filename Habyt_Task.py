# importing required libs
import requests
import json
import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,monotonically_increasing_id,lit

# Initiating spark session for the application
spark = SparkSession.builder.master("local[1]").appName("Habyt").getOrCreate()

# function used for fetching the data from API
def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception if statsu is non 200
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to create the Dataframe from json which is coming from the api
def create_dataframe_from_json(spark, json_data):
    if json_data:
        rdd = spark.sparkContext.parallelize([json.dumps(json_data)])
        df = spark.read.json(rdd)
        return df
    else:
        print("JSON data is None. Unable to create DataFrame.")
        return None

# creating the main dataframe which will have all the listing data for all the cities from the cities API.
def main_dataframecreation(cities_df_c):
    for row in cities_df_c.collect():
        ct_name = row["urlSlug"]
        API_URL_Listings = f"https://www.common.com/cmn-api/listings/common?city={ct_name}"
        print(API_URL_Listings)
        response = requests.get(API_URL_Listings)
        data = response.json()
        rdd = spark.sparkContext.parallelize([json.dumps(data)])
        df = spark.read.json(rdd)
        df = df.cache()
    print(df.count())
    return df


# Property DF Creation from API
def property(df_p):
    if df_p:
        Property_df = df_p.select(col("propertyId").alias("py_id"),
                          col("marketingName").alias("py_marketingname"),
                          col("timezone").alias("py_timezone"),
                          col("brand").alias("py_brand"),
                          col("minGrossRent").alias("py_mingrossrent"),
                          col("minTerm").alias("py_minterm"),
                          col("minMoveInDate").alias("py_minmoveindate"),
                          col("minNetRentCents").alias("py_minnetrentcents"),
                          col("maxNetRentCents").alias("py_maxnetrentcents"),
                          col("minGrossRentCents").alias("py_mingrossrentcents"),
                          col("name").alias("py_name"),
                          col("urlSlug").alias("py_urslug"),
                          col("apiHomeName").alias("py_apihomename"),
                          col("overviewDescription").alias("py_overviewdescription"),
                          col("brandDisplay").alias("py_branddisplay"),
                          col("displayBrandFirst").alias("py_displaybrandfirst"),
                          col("weight").alias("py_weight"))
        Property_df.drop_duplicates()
        print("======Printing Property Data=========")
        Property_df.show(2)
        return Property_df
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

# Listing Dataframe creation from API
def listings(df):
    if df:
        Listings_df = df.select(col("id").alias("ls_id"),
                        col("propertyId").alias("py_id"),
                        col("propertyName").alias("ls_propertyname"),
                        col("marketingName").alias("ls_marketingname"),
                        col("neighborhood").alias("ls_neighborhood"),
                        col("currencyCode").alias("ls_currencycode"),
                        col("occupancyType").alias("ls_occupancytype"),
                        col("description").alias("ls_description"),
                        col("neighborhoodDescription").alias("ls_neighborhooddescription"),
                        col("bedrooms").alias("ls_bedrooms"),
                        col("listingSqft").alias("ls_listingsqft"),
                        col("unitSqft").alias("ls_unitsqft"),
                        col("availableDate").alias("ls_availabledate"))

        Listings_df.drop_duplicates()
        print("======Printing Listing Data=========")
        Listings_df.show(2)
        return Listings_df
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

# flatten Data for Fees and load into Dataframe
def fees(df):
    if df:
        exploded_df = df.select(explode("fees").alias("fee"), col("id").alias("ls_id"))
        exp_fees_df = exploded_df.select(monotonically_increasing_id().alias("fe_id"),
                                 col("ls_id").alias("ls_id"),
                                 col("fee.name").alias("fe_name"),
                                 col("fee.description").alias("fe_description"),
                                 col("fee.amount").alias("fe_amount"),
                                 col("fee.isMandatory").alias("fe_isMandatory"),
                                 col("fee.isRefundable").alias("fe_isRefundable"))

        exp_fees_count= exp_fees_df.count()
        exp_fees_df = exp_fees_df.drop_duplicates()
        print("======Printing Fees Data=========")
        exp_fees_df.show(1)
        return exp_fees_df
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

# Getting the Pricing Data and then exploding it on monthly pricing and creating a clean pricing dataframe####
def price(df):
    if df:
        df_price = df.select(col("id").alias("ls_id"),
                         col("pricing.minimumPrice").alias("pr_minimumPrice"),
                         col("pricing.maximumPrice").alias("pr_maximumPrice"),
                         col("pricing.minimumStay").alias("pr_minimumStay"),
                         col("pricing.monthlyPricing").alias("monthly_pricing")
                         )



        exploded_df_monthly_pricing = df_price.select(explode("monthly_pricing").alias("monthly_pricing"),
                                              col("pr_minimumPrice"),
                                              col("pr_maximumPrice"),
                                              col("pr_minimumStay"),
                                              col("ls_id"))

        extracted_df_pricing_det = exploded_df_monthly_pricing.select(monotonically_increasing_id().alias("pr_id"),
                                                              col("monthly_pricing.name").alias("pr_name"),
                                                              col("monthly_pricing.months").alias("pr_months"),
                                                              col("monthly_pricing.amount").alias("pr_amount"),
                                                              col("monthly_pricing.concessionsApplied").alias("pr_concessionsApplied"),
                                                              col("pr_minimumPrice"),
                                                              col("pr_maximumPrice"),
                                                              col("pr_minimumStay"),
                                                              col("ls_id")
                                                              )

        print("======Printing Pricing Data=========")
        extracted_df_pricing_det.show(2)
        return extracted_df_pricing_det
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

def concessions(price_df):
    if price_df:
        concessions_df = price_df.withColumn("cos_id" , monotonically_increasing_id()) \
                          .withColumn("pr_id", col("pr_id")) \
                          .withColumn("pr_concessionsApplied", col("pr_concessionsApplied")) \
                          .withColumn("cos_amount", lit("")) \
                          .withColumn("cos_description", lit("")).select(col("cos_id"),col("pr_id"),col("pr_concessionsApplied"),col("cos_amount"),col("cos_description"))
        print("======Printing Concessions Data=========")
        concessions_df.show(2)
        return concessions_df
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

# Getting the Address Data and picking the required fields to load in Address Dataframe
def address(df):
    if df:
        Address_df = df.select(monotonically_increasing_id().alias("ad_id"),
                       col("Address.fullAddress").alias("ad_fullAddress"),
                       col("Address.roomNumber").alias("ad_roomno"),
                       col("Address.streetAddress").alias("ad_streetaddress"),
                       col("Address.city").alias("ad_cityname"),
                       col("Address.stateCode").alias("ad_statecode"),
                       col("Address.postalCode").alias("ad_zip"),
                       col("Address.countryCode").alias("ad_countrycode"),
                       col("Address.latitude").alias("ad_lat"),
                       col("Address.longitude").alias("ad_log"),
                       col("Address.belongedCity").alias("ad_bcity"),
                       col("propertyId").alias("py_id"))


        Address_df.drop_duplicates()
        print("======Printing Address Data=========")
        Address_df.show(3)
        return Address_df
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None

# Getting the Cities Data and picking the required fields to load in Cities Dataframe
def cities(df_c):
    if df_c:
        cities_df_c = df_c.select(
        monotonically_increasing_id().alias("ct_id"),
        col("name").alias("ct_name"),
        col("header").alias("ct_header"),
        col("urlSlug").alias("ct_urlslug"),
        col("salesforceIdentifier").alias("ct_salesforceidentifier")
        )

        cities_df_c.drop_duplicates()
        print("======Printing Cities Data=========")
        cities_df_c.show(2)
        cities_df_c.persist()
        return cities_df_c
    else:
        print("DataFrame is None. Unable to process deposit data.")
        return None
# Getting the Neighborhood Data and picking the required fields to load in Neighborhood Dataframe

def neighborhood(df_n,cities_df):
    if df_n and cities_df:
        neighborhoods_df = df_n.select(explode("homes").alias("homes"),
                               monotonically_increasing_id().alias("nh_id"),
                               col("name").alias("nh_name"),
                               col("urlSlug").alias("nh_urlslug"),
                               col("cityUrlSlug").alias("nh_cityurlslug"))


        neighborhoods_df = neighborhoods_df.select(col("homes.propertyId").alias("py_id"),col("nh_id"),col("nh_name"),col("nh_urlslug"),col("nh_cityurlslug"))


        joined_df = neighborhoods_df.join(cities_df, neighborhoods_df["nh_cityurlslug"] == cities_df["ct_urlslug"], "left")

        neighborhoods_df_final = joined_df.select(col("nh_id"),col("ct_id"),col("nh_name"),col("nh_urlslug"),col("nh_cityurlslug"),col("py_id"))

        neighborhoods_df_final.drop_duplicates()
        print("======Printing Neighborhood Data=========")
        neighborhoods_df_final.show(3)
        return neighborhoods_df_final
    else:
        print("DataFrame is None. Unable to process neighborhood data.")
        return None
# Getting the Images Data and picking the required fields to load in Images Dataframe
def images(df):
    if df:
        images_df_exp = df.select(explode("images").alias("images"),col("id").alias("ls_id"))

        images_df = images_df_exp.select(monotonically_increasing_id().alias("im_id"),col("ls_id"),col("images.url").alias("url"),col("images.tag").alias("tag"))
        print("======Printing images Data=========")
        images_df.show(2)
        return images_df
    else:
        print("DataFrame is None. Unable to process neighborhood data.")
        return None
# Getting the Applicant Data and picking the required fields to load in Applicant Dataframe
def applicant(df):
    if df:
        applicant_df_add = df.withColumn("ap_id", monotonically_increasing_id()) \
        .withColumn("ls_id", col("id")) \
        .withColumn("py_id", col("propertyId")) \
        .withColumn("ap_name", lit("")) \
        .withColumn("ap_contactnumber", lit("")) \
        .withColumn("ap_email", lit(""))

        applicant_df = applicant_df_add.select(col("ap_id"),col("ls_id"),col("py_id"),col("ap_name"),col("ap_contactnumber"),col("ap_email"))
        print("======Printing applicant Data=========")
        applicant_df.show(2)
        return applicant_df
    else:
        print("DataFrame is None. Unable to process neighborhood data.")
        return None

# Getting the Deposit Data and picking the required fields to load in Deposit Dataframe
def deposit(applicant_df):
    if applicant_df:
        deposit_df = applicant_df.withColumn("dp_id",monotonically_increasing_id()) \
        .withColumn("dp_amount",lit("")) \
        .withColumn("dp_type",lit("")) \
        .withColumn("ap_id",col("ap_id")).select(col("dp_id"),col("dp_amount"),col("dp_type"),col("ap_id"))

        print("======Printing Deposit Data=========")
        deposit_df.show(2)
        return deposit_df
    else:
        print("DataFrame is None. Unable to process neighborhood data.")
        return None

# Getting the Contract Data and picking the required fields to load in Contract Dataframe
def contract(applicant_df):
    if applicant_df:
        contract_df = applicant_df.withColumn("co_id",monotonically_increasing_id()) \
              .withColumn("co_startdate",lit("")) \
              .withColumn("co_enddate",lit("")) \
              .withColumn("co_terms",lit("")) \
              .withColumn("ap_id",col("ap_id")).select(col("co_id"),col("co_startdate"),col("co_enddate"),col("co_terms"),col("ap_id"))
        print("======Printing Contract Data=========")
        contract_df.show(2)
        return contract_df
    else:
        print("DataFrame is None. Unable to process neighborhood data.")
        return None

def write_dataframe_to_csv(df, file_name):
    file_path = f"/app/{file_name}.csv"
    print("===Start writing files")
    df = df.toPandas()
    #Write the DataFrame to CSV with headers
    df.to_csv(file_path,index=False,header=True)
    print(f"DataFrame written to: {file_path}")

if __name__ == '__main__':
    API_URL_Cities = "https://www.common.com/cmn-api/cities?isPreview=true"

    # get cities data by hitting the cities API using function fetch_data_from_api
    data_cities = fetch_data_from_api(API_URL_Cities)

    # creating the dataframe calling create_dataframe_from_json function for cities
    cities_df_c = create_dataframe_from_json(spark, data_cities)

    # show the cities data
    cities_df_c.show(2)

    # using the list of cities , iterating over each city and hitting the listing API and load the chunk in dataframe
    df = main_dataframecreation(cities_df_c)

    # Defining Neighborhood api
    API_URL_Neighborhoods = "https://www.common.com/cmn-api/neighborhoods?isPreview=true"

    # get neighbourhood data by hitting the neighbourhood API using function fetch_data_from_api
    data_neighborhood = fetch_data_from_api(API_URL_Neighborhoods)

    # creating the dataframe calling create_dataframe_from_json function for neighborhood
    df_n = create_dataframe_from_json(spark, data_neighborhood)

    # Defining Property api
    API_URL_Property = "https://www.common.com/cmn-api/properties"

    # get Property data by hitting the neighbourhood API using function fetch_data_from_api
    data_Property = fetch_data_from_api(API_URL_Property)

    # creating the dataframe calling create_dataframe_from_json function for Property
    df_p = create_dataframe_from_json(spark, data_Property)
    # Getting Property Dataframe
    property_df = property(df_p)
    # Getting listings Dataframe
    listings_df = listings(df)
    # Getting fees Dataframe
    fees_df = fees(df)
    # Getting Price Dataframe
    price_df = price(df)
    # Getting concessions Dataframe
    concessions_df = concessions(price_df)
    # Getting address Dataframe
    address_df = address(df)
    # Getting cities Dataframe
    cities_df = cities(cities_df_c)
    # Getting neighborhood Dataframe
    neighborhood_df = neighborhood(df_n,cities_df)
    # Getting images Dataframe
    images_df = images(df)
    # Getting applicant Dataframe
    applicant_df = applicant(df)
    # Getting deposit Dataframe
    deposit_df = deposit(applicant_df)
    # Getting contract Dataframe
    contract_df = contract(applicant_df)
    dataframe_list = [(property_df, "property"),
                      (listings_df, "listings"),
                      (fees_df, "fees"),
                      (price_df, "price"),
                      (concessions_df,"concessions"),
                      (address_df, "address"),
                      (cities_df, "cities"),
                      (neighborhood_df, "neighborhood"),
                      (images_df, "images"),
                      (applicant_df, "applicant"),
                      (deposit_df, "deposit"),
                      (contract_df, "contract")]

    for row1 in dataframe_list:
        write_dataframe_to_csv(row1[0],row1[1])

