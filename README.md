# pdf-searcher

This repository contains code to scan through thousands of journal articles and identify if and where they contain the word "placebo test".

## 1. Install MySQL

As the purpose of the repository is to scan through over 10,000 pdf files, it is most feasible to employ multiprocessing. As such, a SQL database is preferred over e.g. exporting a Pandas dataframe. With a SQL database we can insert results into a table from the different processes simultaneously.

Install MySQL Community Server at the following link:

https://dev.mysql.com/downloads/mysql/

Make sure to have an account set up, as the credentials are needed in step 3. 



## 2. Install the Virtual Environment

A conda virtual environment to run the code has been exorted to the file `environment.yml`. To install it, first install miniconda or anaconda. Then run the following code in your command line:

`conda env create -f environment.yml `

This will create a virtual environment called `pdf-search`. Activate the environment my running:

`conda activate pdf-search`

With the virtual environment activated, you are now ready to run the python code.



## 3. Config File

Before running code you also need to enter your SQL credentials into the YAML config file. The file is found at `config/config.yml`. Fill in the file as follows:

```
SQL:
  HOST: localhost   # your host 
  USER: 						# your sql user
  PASSWORD:					# your sql password
  DB_NAME: pdf_search   # name of sql DB
```



## 4. Data

In order to run the code you need to populate the data directory. Make sure to populate it in the following way

```
Data <dir>
|- American Economic Review <dir>
|  |- PDFs <dir>
|     |- 2009_1_1 <dir: year_volume_issue>
|        |- file1.pdf
|        |- file2.pdf
|     |- 2009_1_2 <dir
|     |- ...
|- Econometrica
|  |- PDFs <dir>
|   |- 2009_1_1 <dir: year_volume_issue>
|        |- file1.pdf
|        |- file2.pdf
|     |- 2009_1_2 <dir
|     |- ...

		
```



## 5. Run the Code

You run the code by running the file `search_placebo.py` in your Python IDE or command prompt. Searching through 10,000 PDFs took just over 1 hour with a 12-core MacBook Pro. 

Last replicated: 2024-02-14
