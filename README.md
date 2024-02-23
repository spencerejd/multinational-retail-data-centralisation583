# Multinational Retail Data Centralisation

The purpose of this project is to demonstrate a way to produce a system that stores a company's data in a database that is accessible from a centralised location.

## Table of Contents
- [Project Description](#project-description)
    - [Data](#data)
    - [Dependencies](#dependencies)
- [Installation instructions](#installation-instructions)
    - [Get Started](#get-started)
    - [Connect to the local database using pgAdmin4](#connect-to-the-local-database-using-pgadmin4)
- ## File structure


## Project Description
You work for a multinational company that sells various goods across the globe.
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.
In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.
Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.
You will then query the database to get up-to-date metrics for the business.

### Data
The sales data acquired in this project were located in different data sources with information on `users`, `card_details`, `store_details`, `products`, `orders_table`, `date_times`

The data was extracted from:
- Two tables from an SQL database hosted on AWS RDS
- One table stored as a .pdf file hosted on AWS S3
- One table stored as a .csv file hosted on AWS S3
- One table stored as a .json file hosted on AWS S3
- A series of JSON objects available via an API

### Dependencies
[**pandas**](https://pandas.pydata.org/) is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool,
built on top of the Python programming language.

[**numPy**](https://numpy.org/) is a library for the Python programming language, adding support for large, multi-dimensional arrays and matrices, along with a large collection of high-level mathematical functions to operate on these arrays.

[**SQLAlchemy**](https://www.sqlalchemy.org/) SQLAlchemy is the Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL. It is designed for efficient and high-performing database access, adapted into a simple and Pythonic domain language.

[**PyYAML**](https://pypi.org/project/PyYAML/) is used to read the YAML files that contain the database credentials in this project.

[**psycopg2**](https://pypi.org/project/psycopg2/) is a PostgreSQL database adapter for the Python programming language. It enables a connection to the database within Python.

[**boto3**](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) is the AWS SDK for Python to create, configure and manage AWS services.

[**requests**](https://pypi.org/project/requests/) allows you to send HTTP/1.1 requests and is used to make HTTPS GET requests in this project.

[**tabula-py**](https://pypi.org/project/tabula-py/) is a simple Python wrapper of tabula-java, which can read tables in a PDF and convert them into a pandas DataFrame.

## Installation instructions

### Get Started
To get started with the project, follow these steps to set up your environment:

**Clone the repository**

```
git clone https://github.com/spencerejd/multinational-retail-data-centralisation583
cd multinational-retail-data-centralisation939
```

**Setup virtual environment**

```
# On Unix or MacOS
python3 -m venv venv

# On Windows
python -m venv venv
```

**Activate virtual environment**

```
# On Unix or MacOS
source venv/bin/activate

# On Windows
.\venv\Scripts\activate
```

**Install the dependencies**

```
pip install -r requirements.txt
```

### Connect to the local database using pgAdmin4
pgAdmin is used to connect to the local database. With pgAdmin installed and running, we created a database to upload the data to with these steps:

1. On the main application page, right click on Databases. Select Create -> Database.
2. On the 'General' tab of the dialogue that appears, enter a name for the new server connection

## File structure
The project is structured as follows:

- **/scripts**: Contains source code files
    - `database_utils.py`: Utility functions for database operations such as connecting to data sources
    - `data_extractor.py`: Handles data extraction
    - `data_cleaning.py`: Manages data cleaning once data has been extracted from source

- **/ETL_Pipeline**: contains files that extract, clean and upload tables to SQL
    - `extract_clean_card_data.ipynb`
    - `extract_clean_card_data.py`
    - `extract_clean_date_times.py`
    - `extract_clean_orders.py`
    - `extract_clean_products.ipynb`
    - `extract_clean_products.py`
    - `extract_clean_store_details.ipynb`
    - `extract_clean_store_details.py`
    - `extract_clean_user_data.ipynb`

- **/SQL_table_alterations**: SQL files to alter the tables in the database
    - `dim_card_details_schema.ipynb`
    - `dim_date_times_schema.ipynb`
    - `dim_products_schema.ipynb`
    - `dim_store_details_schema.ipynb`
    - `dim_users_schema.ipynb`
    - `set_primary_key.ipynb`
    - `set_foreign_keys.ipynb`
    - `orders_table_schema.py`

- **SQL_Queries/SQL_Queries.ipynb**: specifies SQL code to query the database and generate insights

- **.gitignore**: Specifies files and directories to ignore in Git version control

- **main.py**: responsible for running all the table cleaning and upload operations and the SQL table alterations for the project

- **README.md**: Documentation of the project

- **requirements.txt**: File containing modules to install
