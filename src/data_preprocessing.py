import pandas as pd
import logging
import os

logger = logging.getLogger("data_preprocessing.task")


def load_data(**kwargs):
    """
    Loads data from a CSV file, serializes it, and returns the serialized data.

    Returns:
        bytes: Serialized data.
    """

    logger.info("Starting Load data task")

    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data/zillow_data.csv'))
    del df
    # serialized_data = pickle.dumps(df)
    #
    # return serialized_data


def data_preprocessing(**kwargs):
    """
    Deserializes data, performs data preprocessing, and returns serialized clustered data.

    Args:
        data (bytes): Serialized data to be deserialized and processed.

    Returns:
        bytes: Serialized clustered data.
    """

    logger.info("Starting data preprocessing task")

    # Construct file path
    file_name = os.path.join(os.path.dirname(__file__), '../data/zillow_data.csv')
    logger.info(f"File path set to {file_name}")
    start_date = '2012-01-01'  # Example start date

    # Define the data types for more efficient memory usage
    dtypes = {
        'indicator_id': 'object',
        'region_id': 'int32',
        'value': 'float32',
        'date': 'object'
    }

    # Initialize a list to hold chunks of the processed dataframe
    df_list = []
    count = 0
    # Process the CSV in chunks
    for chunk in pd.read_csv(file_name, chunksize=1000000,
                             usecols=['indicator_id', 'region_id', 'date', 'value'],
                             dtype=dtypes,
                             parse_dates=['date']):
        # Filter the chunk and append to the list
        filtered_chunk = chunk[(chunk['date'] >= start_date)]
        # filtered_chunk.to_csv(f'/Users/dheeraj/Desktop/mlops data/df_chunk_{count}.csv')
        #     print(count)
        logger.info(f"chunk:  {count} reading done")
        count = count + 1
        df_list.append(filtered_chunk)
    logger.info("Reading chunks finished")
    # Concatenate the filtered chunks into one dataframe
    trimmed_df = pd.concat(df_list, ignore_index=True)
    logger.info("Chunks have been concatenated")

    # Save the filtered data to a new CSV file
    trimmed_df.to_csv(os.path.join(os.path.dirname(__file__), '../artifacts/trimmed_dataset.csv'), index=False)
    logger.info("Trimmed Dataframe stored successfully")

    del df_list
    del chunk
    del trimmed_df
    # del df_filtered
    # trimmed_data = pickle.dumps(trimmed_df)
    # return trimmed_data


def get_year_month(**kwargs):
    # trimmed_df = pickle.loads(data)

    logger.info("Starting Get year month task")
    trimmed_df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../artifacts/trimmed_dataset.csv'))

    trimmed_df['year'] = pd.to_datetime(trimmed_df['date']).dt.year
    trimmed_df['month'] = pd.to_datetime(trimmed_df['date']).dt.month

    logger.info("Year and month extraction done")

    trimmed_df.to_csv(os.path.join(os.path.dirname(__file__), '../artifacts/trimmed_dataset.csv'), index=False)
    logger.info("File Saving done")
    del trimmed_df
    # trimmed_data = pickle.dumps(trimmed_df)
    # return trimmed_data


def get_stats(**kwargs):
    # trimmed_df = pickle.loads(data)

    logger.info("Starting get_stats task")
    trimmed_df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../artifacts/trimmed_dataset.csv'))
    # getting just stats

    interested_indicators_stats = ['IRAM', 'CRAM', 'MRAM', 'LRAM', 'NRAM', 'SRAM']

    stat_df = trimmed_df[trimmed_df['indicator_id'].isin(interested_indicators_stats)]

    logger.info("Stats trimming done")

    stat_pivot_df = stat_df.pivot_table(index=['region_id', 'year', 'month'], columns='indicator_id', values='value',
                                        aggfunc='mean')

    logger.info("Pivoting the table done")

    # Reset the index to have a flat DataFrame
    stat_pivot_df.reset_index(inplace=True)
    stat_pivot_df.dropna(inplace=True)

    logger.info("Na's has been dropped")

    del stat_df
    del trimmed_df
    stat_pivot_df.to_csv(os.path.join(os.path.dirname(__file__), '../artifacts/stat.csv'))
    logger.info("File Saving done")
    # trimmed_data = pickle.dumps(trimmed_df)
    # return trimmed_data


def get_merge(**kwargs):
    # trimmed_df = pickle.loads(data)

    logger.info("Starting get merge task")
    trimmed_df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../artifacts/trimmed_dataset.csv'))
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../artifacts/stat.csv'))

    logger.info("Datasets reading done")

    interested_indicators_ZHVI = ['ZATT', 'ZSFH', 'ZALL', 'ZCON', 'ZABT', 'Z2BR', 'Z5BR', 'Z3BR', 'Z1BR', 'Z4BR']

    ZHVI_df = trimmed_df[trimmed_df['indicator_id'].isin(interested_indicators_ZHVI)]

    logger.info("House values Trimming done")

    del trimmed_df

    final_df = pd.merge(ZHVI_df, df, on=['region_id', 'year', 'month'], how='inner')
    logger.info("Final df done")

    # final_data = pickle.dumps(final_df)
    final_df.to_csv(os.path.join(os.path.dirname(__file__), '../data/final.csv'))
    logger.info("File Saving done")
    # return final_data


data_preprocessing()
get_year_month()
get_stats()
get_merge()
