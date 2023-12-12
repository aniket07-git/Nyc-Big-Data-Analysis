# NYC Big Data Analysis Project

## Overview

The NYC Big Data Analysis project is a comprehensive data processing pipeline designed to handle and analyze large datasets related to New York City. This project utilizes Apache Spark (Pyspark), Hadoop, Jupyter Notebook, Python and its libraries such as Pandas and Matplotlib to process, clean, and analyze data, with a focus on efficiency and scalability.

## Prerequisites

Before running this project, ensure that you have the following installed:

- Python 3.6 or later
- PySpark
- Python libraries: pyspark, dotenv
- Hadoop
- Jupyter Notebooks

## Apache Spark Installation - V3.5.0:

Install Apache Spark into your system:

Refer video tutorial below :

https://www.youtube.com/watch?v=LcRxQjTdD1o

### Installation

To install Python, Pyspark, and dotenv, use the command below:

```bash
pip3 install pyspark python-dotenv
```
Install Jupyter Notebooks

```bash
pip3 install notebook
```

This command will install Jupyter Notebook along with its dependencies. Once the installation is complete, you can launch Jupyter Notebook by running:

```bash
jupyter notebook
```

Clone the repository or download the source code. Install the required Python packages as stated above. Set up Apache Spark and Hadoop on your local machine or a cluster.

## Hadoop Installation Tutorials

- **MacOS Users**: [Hadoop Installation on MacOS](https://www.youtube.com/watch?v=H999fIuymqc&t=541s)
- **Windows Users**: [Hadoop Installation on Windows](https://www.youtube.com/watch?v=knAS0w-jiUk)
- **Ubuntu Users**: [Hadoop Installation on Ubuntu](https://www.youtube.com/watch?v=Slbi-uzPtnw)


### Configuration

Create a .env file in your project root directory and add the following entries, replacing the placeholders with your specific paths and configurations:

```bash
# Path to the script to stop Hadoop services
STOP_SERVICES=/path/to/hadoop/sbin/stop-all.sh
# Example: /opt/homebrew/Cellar/hadoop/3.3.6/sbin/stop-all.sh

# Path to the script to start Hadoop services
START_SERVICES=/path/to/hadoop/sbin/start-all.sh
# Example: /opt/homebrew/Cellar/hadoop/3.3.6/sbin/start-all.sh

# Output directory for HDFS
HDFS_OUTPUT_DIR=hdfs://localhost:9000/path_to_your_directory
# Example: HDFS_OUTPUT_DIR=hdfs://localhost:9000/tmp/hadoop-yuvrajpatadia/dfs/data
```

## Usage
The project consists of multiple scripts, each serving a different purpose:

## Script Categories

- **Data Processing Scripts**: Handle reading, cleaning, merging, and transforming data using Apache Spark.
- **Hadoop Service Management**: Manage Hadoop services, including starting and stopping services as needed.
- **Utility Scripts**: Helper scripts that perform tasks like deleting files, running external commands, etc.

Before running the main application esnure you have started Pyspark:

You can run Pyspark manually through your terminal:

Navigate to the folder where you have installed Pyspark:

Run:

```bash
bin/pyspark
```
Example : yuvrajpatadia@Yuvrajs-MacBook-Air spark-3.5.0-bin-hadoop3 

To run the main application, navigate to the root folder of the project and run:

```bash
python3 scripts/main.py

```

This command will launch the entire project, start all Hadoop clusters, and perform tasks like read, clean, merge, etc.



## Workflow 

Once the project is launched, it automatically combines, cleans, and merges all the data and stores it in HDFS. We then use Jupyter Notebook along with Spark SQL to aggregate the data and perform various visualizations.

In Jupyter Notebook, for every cell, you will need to add the following path configuration:

```bash
HDFS_OUTPUT_DIR = 'hdfs://localhost:9000/path_to_your_directory'
# Example: HDFS_OUTPUT_DIR = 'hdfs://localhost:9000/tmp/hadoop-yuvrajpatadia/dfs/data'
```

## Features

- **Data Ingestion and Cleaning**: Scripts to load and clean data from various sources.
- **Data Transformation**: Transform and prepare data for analysis.
- **Hadoop Integration**: Manage Hadoop services directly from the scripts.
- **Logging**: Comprehensive logging to track the data processing flow and errors.

## Contributing

Contributions to the project are welcome. Please follow the standard fork-pull request workflow.








