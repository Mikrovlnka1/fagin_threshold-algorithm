import pandas as pd
from tabulate import tabulate
import time
import create_sorted_csv as csdv



"""
 helper function for computation of time duration for a specified function
"""
def measure_time(func, *args, **kwargs):
    start = time.time()
    result, steps = func(*args, **kwargs)
    end = time.time()
    elapsed_ms = (end - start) * 1000.0  # convert seconds to ms
    return result, elapsed_ms, steps


#----------------------------------------
#----------------------------------------
#implementation of algorithm below...
# ---------------------------------------
# ---------------------------------------.

"""
 read csv data with Pandas library and return as a list of dictionaries
"""
def load_data(path):
    data = pd.read_csv(path)
    data_to_dictionary = data.to_dict('records')
    return data_to_dictionary #list of dictionaries.. [{model: ...} , {...}]



"""
 function to find minimal and maximum valuue for each column. it will help us with normalization
"""
def find_min_max(data, columns):
    min_max_dict = {}
    for column in columns:
        #values = [row_[column] for row_ in data_dic]
        values = []
        for row in data:
            values.append(row[column]) #saving all values of each column into values and then find min a max from each column
        column_min = min(values)
        column_max = max(values)
        min_max_dict[column] = column_min, column_max #saving into dictionary
    return min_max_dict # the return value will be for example: {"price" : (3000,4000), "battery" : (4000,5000)}



"""
function to normalize each column we get as a parameter. the invert_columns will be normalized in different way.
the computation formula: (value - min_of_actual_column) / (max_of_actual_column - min_of_actual_column)

for invert_columns we will do 1 - normalization value.
modifies data in-place.  
"""
def normalize_data(data, columns, invert_columns):
    if invert_columns is None: #if the invert_columns is empty
        invert_columns = [] # we won't invert anything

    min_max_dict = find_min_max(data, columns) #for each wanted column we will get the min and max value.
    #{price:  (100,200), battery: (10,20) ... } for example the return value would look like this :)

    for column in columns: #we will normalize, for each column we will iterate though all lines.
        col_min, col_max = min_max_dict[column] # get the min and max value for the actual column

        for row in data: #iterate through all lines for given column
            val = row[column] # value of column in given line
            x_norm = (val - col_min) / (col_max - col_min) # normalize
            # if the column should be inverted then invert => 1 - x_norm
            if column in invert_columns:
                x_norm = 1.0 - x_norm
            row[column + "_norm"] = x_norm #create new column with normalized value, and name will be in style of name_of_column_norm, for example: price_norm


    #after normalization: [
    #{'model': 'Phone A', 'price': 3000, 'battery': 4000, 'price_norm': 1.0, 'battery_norm': 0.0},


"""
function for computation based on (monotonous) aggregate function 
"""
def compute_score(row, selected_columns, agregation_func):
    score = 0
    #selected_columns are normalized columns , for example: [price_norm, freq_norm] etc.

    values = []
    for col in selected_columns: #iterate through columns
        values.append(row[col])  #all column values on same row will be stored into value

#based on aggregate function we will do our computation
    if agregation_func== "sum":
        return sum(values)
    elif agregation_func== "avg":
        return sum(values) / len(values)
    elif agregation_func== "max":
        return max(values)
    elif agregation_func== "min":
        return min(values)



"""
 implementation of sequence algorithm for finding top K elements based on given properties.
"""
def dumb_algo_top_k(data, k, selected_columns, agregation_func):
    scored = []
    i=0 #for index

    counter = 0 #helper variable for counting steps

    for row in data:  #iterating through each row in data
        score = compute_score(row, selected_columns, agregation_func) # computing the aggregate value
        scored.append((score,i)) #storing into list as a tuple with score and index of the given element
        i+=1
        counter+=1


    scored.sort(key=lambda x: x[0], reverse=True) #sorting

    top_k = scored[:k] #trim it, we just need K elements.

    results = []
    for score, i in top_k: #iterating through list of tuples
        row = data[i].copy() #we will get the values from original data.
        row["score"] = score #adding score column
        counter+=1
        results.append(row)

    return results, counter # return top K elements + number of steps



"""
helper function for laoding sorted csv files. used for fagin and threshhold algorithm using pandas
"""
def load_sorted_csv(path):
    df = pd.read_csv(path)
    df["row_id"] = df["row_id"].astype(int)
    df["value"] = df["value"].astype(float)
    pairs = []
    for _, row in df.iterrows(): # _ is index of line, we dont need it
        rid = int(row["row_id"])  # conversion to int
        val = float(row["value"])
        pairs.append((rid, val))
    return pairs # return list of pairs




"""
implementation of fagin top k algorithm
"""
def fagin_top_k(data, k, sorted_lists,selected_columns, agregation_func):
    amount_of_sub_lists = len(sorted_lists) #amount of given columns
    seen_attributes = set() #set for tracking which columns we saw for each element
    occurances_count = {} # dictionary count of occurrence for each element for example 2: 2.
    seen_attributes_in_all_lists = set() #set for tracking which elements we saw in all columns. so we have everything for them

    i=0
    counter = 0 #for tracking amount of steps


    #sorted_lists is list of lists.
    while True:
        for attribute in sorted_lists: #iterate through all columns in given lists. (parallel read of all columns)
            if i < len(attribute): #check if we are not outside of bounds
                (row_index, value) = attribute[i]
                seen_attributes.add(row_index) #add to set
                occurances_count[row_index] = occurances_count.get(row_index, 0) + 1 #get returns the value or zero if it doesnt exist
                if occurances_count[row_index] == amount_of_sub_lists: # check if we saw the elements everywhere
                    seen_attributes_in_all_lists.add(row_index) #add to list

        i+=1
        counter+=1
        if len(seen_attributes_in_all_lists) >= k: #if we saw K elements everywhere, we can finish
            break
        if i >= len(sorted_lists[0]):
            break


    scored_list = []

    for idx in seen_attributes: #get all data for each element we have in seen_attributes set and compute aggregation value
        counter +=1
        row = data[idx]
        score_val = compute_score(row, selected_columns, agregation_func) #compute aggregation score
        scored_list.append((score_val, idx))
    scored_list.sort(key=lambda x: x[0], reverse=True) #sort
    top_k = scored_list[:k] #we need only top K.

    results = []
    for (sc, idx) in top_k:
        counter+=1
        rcopy = data[idx].copy()
        rcopy["score"] = sc
        results.append(rcopy)
    return results, counter




"""
helper function for computing the threshold value based on given aggregate function
"""
def compute_threshold(current_line_value, agregation_func):
    if agregation_func == "min":
        threshold = min(current_line_value)
    elif agregation_func == "max":
        threshold = max(current_line_value)
    elif agregation_func == "sum":
        threshold = sum(current_line_value)
    elif agregation_func == "avg":
        threshold = sum(current_line_value) / len(current_line_value)
    else:
        threshold = 111

    return threshold



"""
Implementation of threshold algorithm
"""
def threshhold_top_k(data, k, sorted_lists, selected_columns, agregation_func):
    amount_of_sub_lists = len(sorted_lists) #amount of given columns
    i=0
    visited = set() #set of visited elements
    threshold = 1 #threshold value
    min_of_aggr_value = -1 # minimum score we have currently
    top_k_candidates = []
    counter = 0 # steps

    while True:
        current_line_value = [] #for computation of threshold after reading each row
        if i >= len(sorted_lists[0]):
            break
        for attribute in sorted_lists: # iterate through each sorted list and get i-th row. (Parallel reading)
            if i < len(attribute):
                (row_index, value) = attribute[i] # read the given attribute (tuple)
                current_line_value.append(value)# store the value, we will use it for computing threshold

                if row_index not in visited: #we see element for the first time
                    visited.add(row_index)
                    score_val = compute_score(data[row_index], selected_columns, agregation_func) #we compute aggregation score immediately

                    top_k_candidates.append((row_index, score_val))


        #threshold value
        threshold = compute_threshold(current_line_value, agregation_func)
        counter+=1

        #we have enough elements to sort them and get minimum score to see , if we can end our algorithm
        if (len(top_k_candidates)) >= k:
            top_k_candidates.sort(key=lambda x: x[1], reverse=True)
            top_k_candidates = top_k_candidates[:k]
            min_of_aggr_value = top_k_candidates[-1][1]

        # we have enough elements and also our minimum score is higher or equal to threshold --->>> we can end
        if (len(top_k_candidates) >= k) and (min_of_aggr_value >= threshold):
            break

        i+=1



    #prepare the result
    results = []
    for (idx, val) in top_k_candidates:
        counter+=1
        rcopy = data[idx].copy()
        rcopy["score"] = val
        results.append(rcopy)

    return results, counter

#------------------------------------
# END
# -------------------------------------

#used only for debugging/ testing.


if __name__ == "__main__":
    csv_file = 'data/big_data.csv'
    start = time.time()
    #data_all = load_data(csv_file)   #data predstavuji nase data, je to list slovniku
    #print("Pocet nactenych zaznamu: ", len(data_all))
    data = pd.read_csv(csv_file)
    data_to_dictionary = data.to_dict('records')

    columns_to_norm = ["price", "battery", "ram", "size", "camera_res", "display_freq"]
    invert_columnss = ["price"]
    normalize_data(data_to_dictionary, columns_to_norm, invert_columnss)


    df = pd.DataFrame.from_records(data_to_dictionary)
    columns_for_sorted_csv = ["price_norm", "battery_norm", "ram_norm", "size_norm", "camera_res_norm", "display_freq_norm"]
    selected = ['display_freq_norm', 'battery_norm']
    fagin_columns = [f'data/sorted/sorted__{col}.csv' for col in selected]

    #fagin_columns = ['data/sorted/sorted_display_freq_norm.csv', 'data/sorted/sorted_price_norm.csv']
    sorted_lists = []
    for csv_path in fagin_columns:
        pairs = load_sorted_csv(csv_path)  # vrací list[(value, row_id)]
        sorted_lists.append(pairs)



    top5, seq_time_ms,steps = measure_time(dumb_algo_top_k, data_to_dictionary, 10000, selected, 'avg')
    fag, fag_time_ms,steps_f = measure_time(fagin_top_k, data_to_dictionary,10000, sorted_lists ,selected, 'avg')
    threshold_top_k, thre_time_ms, steps_t = measure_time(threshhold_top_k, data_to_dictionary, 10000, sorted_lists ,selected, 'avg')
  



