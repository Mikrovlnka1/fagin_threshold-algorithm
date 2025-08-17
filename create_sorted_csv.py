import pandas as pd


"""
helper function for creating sorted csv files based on given normalized columns.

outfile parametr is for name convention of csv files.
"""
def create_sorted_csv(data_frame, columns_normalized, outfile='sorted_'): #pandas data_frame


    df_index_reset = data_frame.reset_index(drop=False) #index of the element will be same as in original data

    for column in columns_normalized:
        df_sorted = df_index_reset.sort_values(by=column, ascending=False) #pandas function
        df_out = df_sorted[['index', column]].rename(columns={
            'index': 'row_id',
            column: 'value'
        })
        outfile_to_save = f"data/sorted/{outfile}_{column}.csv" #save as a csv file
        df_out.to_csv(outfile_to_save, index=False)
