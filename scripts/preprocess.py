import os, shutil
import kagglehub
import numpy as np
import pandas as pd
import math

def _download_data():
    data_path = "data"
    
    # Cleaning the folder
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    os.makedirs(data_path, exist_ok=True)

    # Downloading the dataset
    path = kagglehub.dataset_download("patrickzel/flight-delay-and-cancellation-dataset-2019-2023",
                                    force_download=True,
                                    output_dir=data_path)

    # Removing useless files
    if os.path.exists(f"{data_path}/.complete"):
        shutil.rmtree(f"{data_path}/.complete")
        
    if os.path.exists(f"{data_path}/dictionary.html"):
        os.remove(f"{data_path}/dictionary.html")

def _convert_hhmm_to_time(series: pd.Series) -> pd.Series:

    # Convert to int, NaN -> None
    cleaned = series.replace('', np.nan).astype('Int64')
    
    def _to_time(x):
        if pd.isna(x):
            return None
        x = int(x)
        hh = x // 100
        mm = x % 100

        if hh > 23 or mm > 59:
            return None
        return f"{hh:02d}:{mm:02d}:00"
    
    return cleaned.apply(_to_time)

def _convert_miles_to_km(series: pd.Series, round_to: int = 2) -> pd.Series:
    # 1 mile = 1.60934 km
    km = series.astype(float) * 1.60934
    return km.round(round_to)


def preprocess_data():
    # Getting raw data
    _download_data()
    raw_df = pd.read_csv("data/flights_sample_3m.csv")
    
    # Preprocessing data
    print("Preprocessing data...")
    raw_df['DISTANCE'] = _convert_miles_to_km(raw_df['DISTANCE'])

    time_columns = ['DEP_TIME', 'CRS_DEP_TIME', 'CRS_ARR_TIME', 'ARR_TIME', "WHEELS_ON", "WHEELS_OFF"]
    for col in time_columns:
        raw_df[col] = _convert_hhmm_to_time(raw_df[col])
        
    unique_columns = ['FL_DATE', 'DOT_CODE', 'FL_NUMBER', 'ORIGIN', 'DEST', 'CRS_DEP_TIME']    
    clean_df = raw_df.drop_duplicates(subset=unique_columns)
    
    # Creating .csv files for db
    airlines_df = clean_df[['AIRLINE_CODE', 'AIRLINE', 'AIRLINE_DOT', 'DOT_CODE']].rename(columns={
        'AIRLINE_CODE': 'airline_code', 
        'AIRLINE': 'airline_name', 
        'AIRLINE_DOT': 'airline_dot',
        'DOT_CODE': 'dot_code'
    })
    airlines_df.drop_duplicates(inplace=True)
    airlines_df.reset_index(inplace=True, drop=True)    

    airports_origin = clean_df[['ORIGIN', 'ORIGIN_CITY']].rename(columns={
        'ORIGIN': 'iata_code', 
        'ORIGIN_CITY': 'city_name'
    })
    airports_dest = clean_df[['DEST', 'DEST_CITY']].rename(columns={
        'DEST': 'iata_code', 
        'DEST_CITY': 'city_name'
    })
    airports_df = pd.concat([airports_origin, airports_dest], ignore_index=True)
    airports_df.drop_duplicates(inplace=True)
    airports_df = airports_df[airports_df['city_name'] != 'CONCORD, NC']    
    airports_df.reset_index(inplace=True, drop=True)
    
    flights_df = clean_df.drop(columns=['AIRLINE', 'AIRLINE_DOT', 'DOT_CODE', 'ORIGIN_CITY', 'DEST_CITY'])
    flights_df.rename(columns={
        'AIRLINE_CODE': 'airline_code',
        'FL_NUMBER': 'fl_number',
        'ORIGIN': 'origin',
        'DEST': 'dest',
        
        'FL_DATE': 'fl_date',
        'CRS_DEP_TIME': 'crs_dep_time',
        'DEP_TIME': 'dep_time',
        'CRS_ARR_TIME': 'crs_arr_time',
        'ARR_TIME': 'arr_time',
        
        'DEP_DELAY': 'dep_delay',
        'ARR_DELAY': 'arr_delay',
        'TAXI_OUT': 'taxi_out',
        'WHEELS_OFF': 'wheels_off',
        'WHEELS_ON': 'wheels_on',
        'TAXI_IN': 'taxi_in',
        
        'CANCELLED': 'cancelled',
        'CANCELLATION_CODE': 'cancellation_code',
        'DIVERTED': 'diverted',
        
        'CRS_ELAPSED_TIME': 'crs_elapsed_time',
        'ELAPSED_TIME': 'elapsed_time',
        'AIR_TIME': 'air_time',
        'DISTANCE': 'distance',
       
        'DELAY_DUE_CARRIER': 'delay_due_carrier',
        'DELAY_DUE_WEATHER': 'delay_due_weather', 
        'DELAY_DUE_NAS': 'delay_due_nas', 
        'DELAY_DUE_SECURITY': 'delay_due_security',
        'DELAY_DUE_LATE_AIRCRAFT': 'delay_due_late_aircraft'
    }, inplace=True)
    flights_df = flights_df[[
    "airline_code","origin","dest",
    "fl_number","fl_date",
    "crs_dep_time","dep_time","crs_arr_time","arr_time",
    "dep_delay","arr_delay","taxi_out","wheels_off","wheels_on","taxi_in",
    "cancelled","cancellation_code","diverted",
    "crs_elapsed_time","elapsed_time","air_time","distance",
    "delay_due_carrier","delay_due_weather","delay_due_nas","delay_due_security","delay_due_late_aircraft"
    ]]
    flights_df["cancelled"] = flights_df["cancelled"].apply(lambda x: True if x == 1.0 else False)
    flights_df["diverted"] = flights_df["diverted"].apply(lambda x: True if x == 1.0 else False)
    string_cols = ['dep_time', 'arr_time', 'wheels_off', 'wheels_on', 'cancellation_code']  
    
    # Saving .csv files for db
    if os.path.exists("data/airlines.csv"):
        os.remove("data/airlines.csv")
    airlines_df.to_csv("data/airlines.csv", na_rep=None, encoding='utf-8', index=False)

    if os.path.exists("data/airports.csv"):
        os.remove("data/airports.csv")
    airports_df.to_csv("data/airports.csv", na_rep=None, encoding='utf-8', index=False)
    
    if os.path.exists("data/flights.csv"):
        os.remove("data/flights.csv")
    flights_df.to_csv("data/flights.csv", na_rep=None, encoding='utf-8', index=False)
    
    if os.path.exists("data/flights_sample_3m.csv"):
        os.remove("data/flights_sample_3m.csv")
    
    print(f"Preprocess finished!\nFind preprocessed .csv files in data/\nshapes\n  airlines: {airlines_df.shape}\n  airports: {airports_df.shape}\n  flights: {flights_df.shape}")

if __name__ == "__main__":
    preprocess_data()