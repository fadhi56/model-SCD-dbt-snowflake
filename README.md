### Project Info
The project is centered around modeling slowly changing dimesnion, particularly SCD2 which is the defacto for SCD when they need to be considered 
for the dimesnional modeling in data warehousing. 

## Data Layers
I am using the conventional medallion architecture (Bronze-silver-gold) to model the different layer of refinements. 

## Data Source (Bronze layer)
The data sources (i am using convetional product, order and customer tables) are directly available as csv file. In the current model, i am using snowflake-adapter and the table are generated using the GUI interface of snowflake and loading directly as table from the file. These table are then defined as source in the bronze layer. 

## SCD 2 (Silver layer)

The SCD2 is modeled for the product and customer dimesnions. Go through the model file to have a deeper look. 

## Gold layer

The gold layer is a simple denormalized data set by combining all the records 

## How to run the mdoels

I used the snowflake trial version to test the models. All you need to do is to repalce the respective parameters in the profiles.yml file and you are
good to go. 

Use following commands to run and test models in silver and gold layer
- dbt run --models models/silver_layer/
- dbt test --models models/silver_layer/

- dbt run --models models/gold_layer/
- dbt test --models models/gold_layer/

## Generic tests

In addition, i have added some generic test under test > generic to test the model. 

## Referential Integrity

In addition, i have added the referential integrity in the the fact table as follows for customer and product table

- name: customer_id
data_type: number
description: " "
data_tests:
    - not_null
    - relationships:
        to: ref('dim_customer')
        field: customer_id

