import xorbits
import xorbits.pandas as pd
import os


def get_stock_data(path_to_dataset):
    stock_data = {}
    total_files = len(os.listdir(path_to_dataset))
    processed_files = 0
    for filename in os.listdir(path_to_dataset)[-1000:]:
        if filename.endswith('.txt'):
            stock_name = filename[:-4]
            try:
                stock_df = pd.read_csv(os.path.join(path_to_dataset, filename), parse_dates=['Date'])
            except Exception:
                print(f"Skipping {filename} due to empty data.")
                continue
            stock_df.set_index('Date', inplace=True)
            stock_df = stock_df.reindex(pd.date_range(start='2015-01-01', end='2015-12-31'))
            stock_df['Return'] = stock_df['Close'].pct_change()
            stock_data[stock_name] = stock_df
        processed_files += 1
        print(f"Processed {processed_files}/{total_files} files.")
    return stock_data


def get_return_matrix(stock_data):
    return_matrix = pd.DataFrame({stock_name: stock_data[stock_name]['Return'] for stock_name in stock_data}, index=pd.date_range(start='2015-01-01', end='2015-12-31'))
    return return_matrix


def get_correlation_matrix(return_matrix):
    correlation_matrix = return_matrix.rolling(window=30).agg('corr')
    return correlation_matrix


xorbits.init("http://127.0.0.1:7778")

path_to_dataset = '/path/to/Stocks'
stock_data = get_stock_data(path_to_dataset)

return_matrix = get_return_matrix(stock_data)
return_matrix.to_csv('returns.csv')

correlation_matrix = get_correlation_matrix(return_matrix)
correlation_matrix.to_csv('corr.csv')

xorbits.shutdown()
