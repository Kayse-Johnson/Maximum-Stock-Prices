import yfinance as yf
import numpy as np
import pandas as pd
tickers = ['AVA','GME','MSFT','INTC','AMZN']

class ShareStrat:
    ''' ShareStrat provides functionality to find stock prices across multiple days and 
        store the maximum profit that could have been made during that time period.

        Attributes:
            tickers : a list of all the tickers considered
            shares : a dictionary storing the share prices as an array for each ticker
            maximum_profits : a dictionary storing the maximum profits for each ticker
    '''
    def __init__(self, tickers, period):
        self.tickers = tickers # store tickers
        self.shares = {} # create empty dict to store arrays from each ticker
        self.maximum_profits = {} # create empty dict to store the max profits from each ticker
        for share in tickers: # iterate through each ticker in the list
            ticker_object = yf.Ticker(share)
            x = ticker_object.history(period=period)
            self.shares[share]=np.round(x['Open'].values).tolist() # store arrays in the dictionary   
    def __len__(self):
        return len(self.tickers) # number of tickers considered
    
    def create_custom_array(self, ticker, array):
        ''' Creates a ticker and array specificed by the user and stores it into the self.shares dictionary.'''
        self.shares[ticker]=array
    
    def _all_possible_transactions(self, location_index1, location_index2, k):
        '''Private method used to iterate through all the location indices and only consider valid combinations of transactions.'''
        transactions = [] # create empty list
        if k ==2:
            for i in location_index1: # iterate through the location indices index which contains all the possible single transactions twice
                for j in location_index2:
                    if not (i[0]==j[0] or i[1]>=j[0]): # ensure rows indices are not equal and the column value for the first index is lower than the row value for the second index
                        transactions.append([i,j])             # i.e (1,3) cannot be followed by (1,4) or (2,3) as those transaction combinations are impossible
            return transactions # return a list of a list of tuples containing all possible transactions that can occur given K = 2 i.e [[(1,2),(3,4)],[(1,2),(3,5)]]

        else: # if k>2
            for i in location_index1: # iterate through location indices 1 which in this case will be all the possible transactions for k>2 i.e [(1,2),(3,4),(5,6)],...
                for j in location_index2: # iterate through the indices array which contains all the possible single transactions
                    if not (i[-1][0]==j[0] or i[-1][1]>=j[0]): # ensure last tuple for each transaction combination list abides by rules similar to when k = 2, further remove impossible transactions
                        transactions.append([*i,j]) # unpack the list of tuples i and create a new list of tuples including tuple j which indicates a legal transaction and append to empty list
            return transactions # return a list of a list of tuples for cases where k > 2
    
    def _k_transactions(self, index, k): 
        ''' Private method used to produce a list indicating all possible transactions across all the number of transactions up to and including K.'''
        total_transactions = [] # empty list for storing all the transactions from k>=2
        transactions = self._all_possible_transactions(index,index,2) # find all possible transactions for when k = 2
        total_transactions.append(transactions) # append these transactions to the total
        if k ==2: # if k = 2 return the total transactions at this point, no need to investigate further
            return total_transactions
        else:
            for i in range(k-2): # if k>2 then starting from 3 find all the possible transactions up to k and at each stage store them into the total transactions list
                transactions = self._all_possible_transactions(transactions, index, i+3)
                total_transactions.append(transactions)
            #print(total_transactions)
            return total_transactions # return the total list of transactions across all k vales - a list of a list of a list of tuples

    def transaction_strategy(self, ticker, k):
        '''Calculates the maximum profit possible in a given array of stock prices given k number of transactions.'''
        difference_matrix = np.zeros((len(self.shares[ticker]),len(self.shares[ticker]))) # create an empty numpy matrix of the same dimensions as the length of the share array
        for i,x in enumerate(self.shares[ticker]): # iterate through each item in the share array and return the value and index
            for j,y in enumerate(self.shares[ticker]):
                if not (i==j or i>j): # check that the index values are not equal and row values are not greater than column values - do not include impossible transactions i.e (0,0),(1,1),(1,0)
                    difference_matrix[i][j] = y-x # produce difference between stock indicated by those two days
        print(difference_matrix)
        index_arrays = np.nonzero(difference_matrix) # Produce 2 np arrays representing rows, columns of non 0 elements from the difference matrix
        difference_matrix = difference_matrix[difference_matrix != 0] # reduce the difference matrix to a single array with all the non 0 difference elements
        print(difference_matrix)
        index = tuple(zip(index_arrays[0],index_arrays[1])) # unpack the seperate numpy arrays into a tuple of tuples where each individual tuple represents a co-ordinate in the difference matrix
        print(index)
        if k == 1: # if k = 1 then immediately return the max value from the array of differences which represents a single transaction
            return max(difference_matrix)
        else: # otherwise recieve all possible transactions up to and including k number of transactions
            total_transactions = self._k_transactions(index, k)
            total_transactions = list(filter(None, total_transactions)) # this removes lists with empty elements which may occur if k values are chosen where there are no possible transactions

            for dim1 in range(len(total_transactions)): # these iterate through each dimension in the total_transactions and for each tuple it finds its index to replace the indices combinations with the actual transaction values 
                for dim2 in range(len(total_transactions[dim1][:])):
                    for dim3,transaction in enumerate(total_transactions[dim1][dim2][:]):
                        ind = index.index(transaction) # finds the the location of the transaction  within the index tuple of tuples
                        total_transactions[dim1][dim2][dim3]=difference_matrix[ind] # since each item in index corresponds to a value it creates total_transactions with the same dimensions but with transaction values instead of co-ordinates
            
            total_transactions = [np.array(total_transactions[i]) for i in range(len(total_transactions))] # recreate total_transactions as a numpy array in order to sum all the transaction values within each possible combination of transactions
            max_array =[] # initialise empty max array which indicates the maximum profit for k >= 2 as an element in the array
            max_array.append(max(difference_matrix)) # append the max value of the difference matrix so that all 1-k number of transactions are considered
            for array in total_transactions: # for each of the arrays in total transactions which represents a different k sum across all the combinations
                array=array.sum(axis=1)
                max_array.append(max(array)) # append the max value to the max_array
            print('\nmax_Array = ',max_array)
            return max(max_array) # return the max value in max_array which represent the maximum possible profit given k transactions
    def transaction_strategies(self, k):
        '''Calculates the maximum profits for all the tickers and stores it into the maximum profits dictionary.'''
        for ticker in self.tickers: # iterate through each of the tickers and store the max profits in a dictionary to be saved with the ticker as the key
            maximum_profits = self.transaction_strategy(ticker, k)
            self.maximum_profits[ticker] = [maximum_profits]
    def store_max_profits(self):
        ''' Stores the maximum profits dictionary in to a parquet format.'''
        pdf = pd.DataFrame(self.maximum_profits)
        df = spark.createDataFrame(pdf)
        df.write.format("parquet").mode("overwrite").save('/data/tmp/max_stock_profits')
    def load_max_profits(self): 
        ''' Loads the maximum profits from the parquet.'''
        df = spark.read.format("parquet").load('/data/tmp/max_stock_profits')
        max_profits = df.toPandas().to_dict()

obj = ShareStrat(tickers,'5d')
#obj.create_custom_array('new',[5,11,3,50,60,90])
print(obj.shares['MSFT'])
#obj.transaction_strategy('AMZN',3)
#print(obj.shares['AMZN'])
#obj.transaction_strategies(3)