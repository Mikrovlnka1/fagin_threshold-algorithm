import streamlit as st
import pandas as pd
import os
from algoritm import (
    dumb_algo_top_k,
    fagin_top_k,
    threshhold_top_k,
    normalize_data,
    compute_score,
    measure_time,
    load_sorted_csv
)

# --- Config ---
CSV_PATH = "data/big_data.csv"
SORTED_PATH = "data/sorted"
COLUMNS_TO_NORMALIZE = ["price", "battery", "ram", "size", "camera_res", "display_freq"]
#INVERT_COLUMNS_DEFAULT = ["price"]
#st.set_page_config(layout="wide")



# --- Initialization with data caching ---
@st.cache_data
def load_and_prepare_data():
    df = pd.read_csv(CSV_PATH)
    data_dict = df.to_dict('records')
    #normalize_data(data_dict, COLUMNS_TO_NORMALIZE, invert_columns)
    return data_dict

@st.cache_data
def load_sorted_lists(selected):
    sorted_lists = []
    for col in selected:
        path = os.path.join(SORTED_PATH, f"sorted__{col}.csv")
        sorted_lists.append(load_sorted_csv(path))
    return sorted_lists


# --- UI ---
st.title("Top(k) Algoritmy nad datasetem mobilních telefonů")

data_dict = load_and_prepare_data()
all_norm_columns = [col + "_norm" for col in COLUMNS_TO_NORMALIZE]

st.subheader("Invertování hodnot atributů")

invert_columns = []

#creating section for inverting normalized data
columns_per_row = 3
rows = [st.columns(columns_per_row) for _ in range((len(COLUMNS_TO_NORMALIZE) + columns_per_row - 1) // columns_per_row)]

for idx, col in enumerate(COLUMNS_TO_NORMALIZE):
    row_idx = idx // columns_per_row
    col_idx = idx % columns_per_row
    with rows[row_idx][col_idx]:
        if st.checkbox(f"{col.capitalize()}", value=(col == "price"), key=f"invert_{col}"):
            invert_columns.append(col)

#normalizing data
normalize_data(data_dict, COLUMNS_TO_NORMALIZE, invert_columns)

#section for selecting columns to search
selected_columns = st.multiselect("Vyber atributy", all_norm_columns, default=["display_freq_norm", "battery_norm"])
aggr_func = st.selectbox("➕ Agregační funkce", ["avg", "sum", "min", "max"])
algorithm = st.radio("🧠 Vyber algoritmus", ["Sekvenční", "Fagin", "Threshold"])
max_k = len(data_dict)


# K-value
k = st.number_input(
    "🔝 Zadej hodnotu k",
    min_value=1,
    max_value=max_k,
    value=min(5, max_k),
    step=1,
    format="%d"
)


#calling our implemented functions in algorithm.py
if st.button("🚀 Spustit algoritmus"):
    if algorithm == "Sekvenční":
        result, exec_time, steps = measure_time(dumb_algo_top_k, data_dict, k, selected_columns, aggr_func)

    elif algorithm == "Fagin":
        sorted_lists = load_sorted_lists(selected_columns)
        result, exec_time, steps = measure_time(fagin_top_k, data_dict, k, sorted_lists, selected_columns, aggr_func)

    elif algorithm == "Threshold":
        sorted_lists = load_sorted_lists(selected_columns)
        result, exec_time, steps = measure_time(threshhold_top_k, data_dict, k, sorted_lists, selected_columns, aggr_func)

    st.success(f"✅ Algoritmus dokončen za {exec_time:.2f} ms")
    st.info(f"Algoritmus skončil po {steps} krocích.")

    result_df = pd.DataFrame(result)

    # get only columns with original data without normalized columns. we don't need them in result.
    cols_to_show = set(col for col in result_df.columns if "_norm" not in col)


    # to list conversion
    cols_to_show = [col for col in result_df.columns if col in cols_to_show]

    # the final output
    display_df = result_df[cols_to_show]
    st.dataframe(display_df, use_container_width=True)