# snowflake_etl_pipeline
This repository contains a simple Python script using the pandas library to extract data from a CSV file, clean and transform the data and load it into a Snowflake table with matching schema.

(Step 1 for Mac or Linux only)
1.Open terminal and run "brew --version" to check if brew is installed, if not, run 
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

2.In terminal, run 'pip --version' to check if pip is installed, pip comes pre-installed with python,
to upgrade pip using python ensurepip module.
For Mac/Linux : python -m ensurepip --upgrade
For Windows : py -m ensurepip --upgrade

3.To install python pandas library, run 'pip install pandas'

4.To install snowflake connector for python, run 'brew install 'snowflake-connector-python' or 'pip install 
snowflake-connector-python' 

5.To install pandas which supports 'write_pandas', run 'brew install 'snowflake-connector-python[pandas]' or
pip install 'snowflake-connector-python[pandas]'

