# redshift-data-modeling
### Udacity Data Engineer Nanodegree project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

### Requirements for running locally
- Python3 
- Docker
- Docker-Compose 

### Project structure explanation
```
postgres-data-modeling
│   README.md                 # Project description
|   dwh.cfg                   # Configuration file
|   requirements.txt          # Python dependencies
│   
└───src                       # Source code
|   |               
│   └───notebooks             # Jupyter notebooks
|   |   |  test.ipynb         # Run sql queries agains Redshift
|   |   |
|   └───scripts
│       │  create_cluster.py  # Cluster creation script
|       |  create_tables.py   # Schema creation script
|       |  delete_cluster.py  # Cluster deletion script
|       |  etl.py             # ETL script
|       |  sql_queries.py     # Definition of all sql queries
```

### Instructions for running locally

Clone repository to local machine
```
git clone https://github.com/brfulu/redshift-data-modeling.git
```

Change directory to local repository
```
cd redshift-data-modeling
```

Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

Run scripts
```
cd src/
python -m scripts.create_clsuter  # create redshift cluster
python -m scripts.create_tables   # create schema
python -m scripts.etl             # load data into staging tables, transform and load into fact and dim tables
```

Check results

```
jupyter notebook  # launch jupyter notebook app

# The notebook interface will appear in a new browser window or tab.
# Navigate to src/notebooks/test.ipynb and run sql queries against Redshift
```