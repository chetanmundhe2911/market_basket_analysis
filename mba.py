#Load libraries
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules

number_of_items = 50_000
number_of_transactions = 40_000_000
number_of_transactions_lines = 200_000_000

#Create a Dataframe from arrays
df = pd.DataFrame({"TransactionID" : np.random.randint(low=1, high = number_of_transactions, \
                                                       size=number_of_transactions_lines),
                  "Item" : np.random.randint(low=1, high = number_of_items, \
                                             size=number_of_transactions_lines),
                  "Quantity" : np.ones(number_of_transactions_lines, dtype = np.int8)})


#Remove dulicates from the DataFrame
df.drop_duplicates(keep='first', inplace=True, ignore_index=False)

#Convert tabular dataframe to Sparse Matrix suitable for Basket Analysis
sparse_matrix = csr_matrix((df.Quantity, (df.TransactionID, df.Item)), shape=(number_of_transactions, number_of_items))
# Replace NAN in Sparse Matrix to Zeroes
sparse_matrix = np.nan_to_num(sparse_matrix, copy=False)

# Create a Sparse DataFrame from Sparse Matrix
df = pd.DataFrame.sparse.from_spmatrix(sparse_matrix)  

# Create a DataFrame with Frequent Itemsets
frequent_itemsets = fpgrowth(df, min_support=0.05, use_colnames=True)

# Chech Support, Confidence and other Metrics by Itemsets
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.7)

#----
