# Quant Data Platform

### Overview
The Quant Data Platform is designed to streamline the process of data collection, extraction, transformation, and loading (ETL). It consists of three main components: 
1. collector
2. etl
3. EDA. 

The platform facilitates the acquisition of data from multiple sources, processes this data, and organizes it into a structured format suitable for analysis and reporting.

### Project Structure
#### Collector
The collector module is responsible for gathering data from two different sources: API and CSV files.
It ensures that the collected data is stored securely in a Data Lake on AWS S3 using Delta format.

1. ad_api_collector.py: Collects data from the specified API.
2. ad_csv_collector.py: Parses and collects data from CSV files.
3. base_collector.py: Provides a base class for the collector modules.

#### ETL

The etl module is composed of extractors, tables, and pipelines that manage the ETL process.

#### Extractors
The extractors sub-module contains scripts to extract data from the Delta Data Lake.

* ad_api_extractor.py: Extracts data from the Data Lake for API-sourced data.
* ad_csv_extractor.py: Extracts data from the Data Lake for CSV-sourced data.
* base_extractor.py: Provides a base class for the extractor modules.

#### Tables
The tables sub-module is tasked with creating the warehouse table schema.

1. ad_table.py: Defines the schema for ad_details table.
2. district_table.py: Defines the schema for district_detail table.
3. property_type_table.py: Defines the schema for property_type table.

#### Pipeline
The pipeline sub-module orchestrates the ETL process, calling upon extractors and tables to generate the final data warehouse structure.

1. ads_data_pipeline.py: Manages the data pipeline for advertisement data.
2. base_pipeline.py: Provides a base class for pipeline modules.

#### EDA
The final warehouse tables are stored in the S3 Delta warehouse. The following tables are generated:

* ad_details: Contains detailed information about the advertisements.
* district_detail: Stores details pertaining to districts.
* property_type: Lists the types of properties.

You can find the pdf of the analysis made on this warehouse, or run the notebook.

### Getting Started
To set up the Quant Data Platform, follow these steps:

* Set up AWS credentials: Ensure that your AWS credentials are configured correctly to allow access to S3 services.

* Install dependencies: Run pip install -r requirements.txt in both the collector and etl directories to install necessary packages.

* Data Collection: Execute the collector scripts to start collecting data.

* Running the ETL process: Navigate to the etl directory and execute the main.py script to perform the ETL operations and populate the data warehouse.

### Contributing
Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests to us.

### Versioning
1.0.0

### Authors
Mohammad Shubaita - Initial work - [My LinkedIn](https://www.linkedin.com/in/mohamadshbaitah/) 

### License
This project is for learning and specific task oriented - see the LICENSE.md file for details.

