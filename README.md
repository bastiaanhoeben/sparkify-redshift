## Sparkify data modeling with AWS Redshift

### About the project

### Overview




### Raw datasets

The raw data to be housed in the data warehouse comprises two datasets: a song dataset and a log dataset. The song dataset consists files in JSON format, each containing metadata about a song. The log dataset consists of log files in JSON format containing app activity logs from a music streaming app.

### Data model

The data is modelled according to the start schema depicted below. The songplays table hereby constitutes the fact table, while the users, artists, songs and time tables are the dimensional tables.

![Star Schema](support_files/star_schema.png)

### Infrastructure design

The ETL pipeline extracts data from an S3, stages the data in Redshift and transforms the data to fit the above described star schema.

### Running instructions

Execute the following steps to get an environment running on your system in which to execute the python scripts and run the ETL pipelines against the AWS services:

1. Clone the repository into the working directory and move into the project
   directory:
   ```
   git clone https://github.com/bastiaanhoeben/sparkify-redshift.git
   ```
   ```
   cd sparkify-redshift
   ```   

2. Create a virtual environment and activate it:
   ```
   python3 -m venv .venv
   ```
   ```
   source .venv/bin/activate
   ```
   
3. Install the necessary packages from requirements.txt:
   ```
   python -m pip install -r requirements.txt
   ```

4. Fill in the *template.cfg* TOML configuration document with the required parameters.

5. Run the *aws_config.py* script to set up a Redshift database instance:
   ```
   python aws_config.py
   ```

6. Run the *create_tables.py* script to create the staging and final tables in Redshift:
   ```
   python create_tables.py
   ```

7. Run the *etl.py* script to load the raw data into the staging tables, transform the data from the staging tables to adhere to the data model and insert the transformed data into the final fact and dimensional tables:
   ```
   python etl.py
   ```

8. Run the *analytics_queries.py* script to perform a number of example queries against the Redshift data warehouse:
   ```
   python analytics_queries.py
   ```