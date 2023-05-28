# Ergo Tokenautics

Welcome to Ergo Tokenautics, a robust, Python-based utility designed to facilitate in-depth analysis of token distribution on the Ergo network.

All gathered data is accumulated daily in the 'data' directory of this repository. For a more user-friendly visualization and exploration of the collected data, please refer to the Ergo Tokenautics Frontend (https://babygrenade.github.io/ergo-tokenautics-frontend/).

## Token Tracking

Our script automatically tracks tokens listed in the `token_list.csv` file. Should you wish to add new tokens to the list, simply append them to this file and create a pull request. Tokens added to the [ergo.json](https://github.com/spectrum-finance/default-token-list/blob/master/src/tokens/ergo.json) in the Spectrum Finance repository will be automatically included in our tracking system.

## Usage

To use this script, clone this repository, add your desired token(s) and their respective ID(s), and then run the `get_token_holders.py` Python script. The script will generate a CSV file containing the distribution data for each token.


## Technical Overview

Our Python script utilizes several advanced libraries and features to effectively scan the Ergo network for data about addresses that hold unspent boxes for tokens. The system relies on asynchronous HTTP requests, efficient data parsing and filtering techniques, and data persistence strategies to accurately track token distribution.

Here's an outline of the technical process:

1. **Asynchronous HTTP Requests:** The script employs the `grequests` library for its inherent ability to manage asynchronous HTTP requests. This facilitates a significant increase in efficiency and speed of data retrieval from the Ergo API.

2. **Data Parsing and Filtering:** For every retrieved box from the Ergo API, the `get_box_amounts` function is used to parse and filter relevant information, specifically the box ID, the associated address, and the token amount. The function accepts the items (unspent boxes) and the token_id as arguments, returning a list of dictionaries for each box with the relevant details.

3. **Data Retrieval:** The primary function `get_holders` manages the complete data retrieval process. This function utilizes the `requests` library alongside the Retry object from `requests.packages.urllib3.util.retry` to manage retries for failed HTTP requests. The function implements a loop that continuously requests data from the Ergo API until all data (for a specific token) is retrieved.

4. **Data Aggregation:** Once all data for a specific token is retrieved, it's aggregated and transformed using the Pandas library into a DataFrame. Here, duplicate entries are removed, and the DataFrame is grouped by address. Additionally, the percentage of each holder's share is calculated and added to the DataFrame.

5. **Data Persistence:** Finally, the script saves the processed data to a CSV file in the 'data' directory.

