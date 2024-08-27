### Project Info
- dbt is running in combination with snowflake
- Contains three layers in model folder, bronze, silver and gold, i.e., first, second and third layer
- CSV files are directly uplaoded to snowflake database to create source
- silver layer model are generated from the source. 
- helping tests are placed in test > generic
- gold layer contains the final DWH layer
- Analytics folder contains sql worksheet for analytics

Use followin command to run and test models in silver and gold layer
- dbt run --models models/silver_layer/
- dbt test --models models/silver_layer/

- dbt run --models models/gold_layer/
- dbt test --models models/gold_layer/