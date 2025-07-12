Creating a data pipeline for cleansing, standardizing, and validating data involves several steps, including reading input data, performing transformations, and validating the results. Below is a Python program that provides a basic framework for this process. This example assumes that the input data is in CSV format, but it can be adapted to other formats as needed.

```python
import pandas as pd
import numpy as np
import os
import logging

# Configure logging to write to a file
logging.basicConfig(filename='data_cleanse.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    """Load data from a CSV file."""
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Data loaded successfully from {file_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pd.errors.ParserError:
        logging.error(f"Error parsing the file: {file_path}")
        raise
    except Exception as e:
        logging.error(f"An unknown error occurred while loading data: {e}")
        raise

def standardize_column_names(data):
    """Standardize column names to lowercase and replace spaces with underscores."""
    try:
        data.columns = [col.strip().lower().replace(' ', '_') for col in data.columns]
        logging.info("Column names standardized.")
        return data
    except Exception as e:
        logging.error(f"An error occurred while standardizing column names: {e}")
        raise

def cleanse_data(data):
    """Cleanse data by handling missing values, duplicates, etc."""
    try:
        # Fill missing values with an appropriate placeholder, e.g., median for numerical columns
        data.fillna(data.median(numeric_only=True), inplace=True)
        data.fillna('', inplace=True)  # for non-numeric columns
        data.drop_duplicates(inplace=True)
        logging.info("Data cleansing completed.")
        return data
    except Exception as e:
        logging.error(f"An error occurred during data cleansing: {e}")
        raise

def validate_data(data):
    """Validate data by checking column types, ranges, etc."""
    try:
        # Check for expected data types
        # Example validation: Check if 'age' column has only positive integers
        if 'age' in data.columns:
            if not np.issubdtype(data['age'].dtype, np.integer):
                raise ValueError("Age column must be integer type")
            if any(data['age'] < 0):
                raise ValueError("Age column contains negative values")

        # Additional validations can be added here
        logging.info("Data validation completed.")
    except ValueError as ve:
        logging.error(f"Data validation error: {ve}")
        raise
    except Exception as e:
        logging.error(f"An unknown error occurred during data validation: {e}")
        raise

def save_data(data, output_path):
    """Save the cleansed and validated data to a new CSV file."""
    try:
        data.to_csv(output_path, index=False)
        logging.info(f"Data saved successfully to {output_path}")
    except Exception as e:
        logging.error(f"An error occurred while saving data: {e}")
        raise

def process_pipeline(input_file, output_file):
    """The main function to orchestrate the data pipeline."""
    try:
        data = load_data(input_file)
        data = standardize_column_names(data)
        data = cleanse_data(data)
        validate_data(data)
        save_data(data, output_file)
        logging.info(f"Data pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == '__main__':
    input_file = 'input_data.csv'
    output_file = 'output_data.csv'
    process_pipeline(input_file, output_file)
```

### Key Aspects

1. **Loading Data**: Reads data from a CSV file, and handles file not found and parsing errors.
2. **Standardizing Column Names**: Ensures column names are in a standardized format.
3. **Cleansing Data**: Handles missing values and duplicates. The example fills numeric missing values with the median and removes duplicates.
4. **Validating Data**: Includes simple validation checks, such as ensuring numerical columns have the correct data type and value constraints.
5. **Saving Processed Data**: Writes the processed data back to a CSV file.
6. **Logging**: Captures the flow of data and any errors using Python's `logging` module, which writes logs to a specified file.

This template can be extended to include more sophisticated data transformations, cleansing strategies, and validations depending on the specific requirements of your data pipeline.