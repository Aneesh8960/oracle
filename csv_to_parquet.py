import pandas as pd
import zipfile
import os

def process_data(data, dtypesdf):
    for column in data.columns:
        expected_dtype = dtypesdf.loc[dtypesdf['COLUMN_NAME'] == column, 'DATA_TYPE'].values[0]
        if expected_dtype.strip().lower() == 'int32':
            data[column] = pd.to_numeric(data[column], errors='coerce').fillna(999999999).astype('Int32')
            data[column].replace({99999: None}, inplace=True)
        elif expected_dtype.strip().lower() == 'int64':
            data[column] = pd.to_numeric(data[column], errors='coerce').fillna(999999999).astype('int64')
            data[column].replace({99999: None}, inplace=True)
        elif expected_dtype.strip().lower() == 'float64':
            data[column] = pd.to_numeric(data[column], errors='coerce').fillna(999999999).astype('float64')
            data[column].replace({99999: None}, inplace=True)
        elif expected_dtype.strip().lower().strip() == 'string':
            data[column] = data[column].astype('object')
        elif expected_dtype.strip().lower() == 'date64':
            data[column] = pd.to_datetime(data[column], errors='coerce').dt.date
        elif data[column].dtype != expected_dtype:
            data[column] = data[column].astype(expected_dtype)
def process_file(metadata, source_dir):
    source_dir = source_dir.strip('"')
    for filename in os.listdir(source_dir):
        print('Processing:', filename)
        date_string = filename.split('_')[-1].split('.')[0]
        if metadata['source_file_type'] == 'zip':
            with zipfile.ZipFile(os.path.join(source_dir, filename), 'r') as z:
                data_filename = z.namelist()[0]
                with z.open(data_filename) as f:
                    df = pd.read_csv(f, sep=metadata['delimeter'], escapechar=metadata['escape_character'], skiprows=int(metadata['line_skip']), engine='python')
        else:
            df = pd.read_csv(os.path.join(source_dir, filename), sep="|",engine='python')
            print(df.columns)
            df['filename'] = filename
            df['event_date'] = date_string
            df['load_date'] = date_string
        df.columns = metadata['columns'].replace('""', '').split(',')
        print(df.columns)
        process_data(df, dtypesdf)
    return df
def write_parquet(df, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    parquet_path = os.path.join(r"C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\omv\ETL", "data.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"DataFrame written to {parquet_path}")
if __name__ == "__main__":
    metadata_df = pd.read_csv(r"C:\Users\AneeshKumar\Downloads\new_mata.csv")
    metadata_df = metadata_df[metadata_df['is_active'] == 1]
    print(metadata_df)
    voce_name='sdp_dump'
    dtypesdf = pd.read_csv(r"C:\Users\AneeshKumar\Downloads\column_details.csv")
    dtypesdf = dtypesdf[dtypesdf['TABLE_NAME'].str.upper() == voce_name.upper()]
    print(dtypesdf)
    for _, metadata in metadata_df.iterrows():
        source_dir = metadata['source_container']
        print('Source directory:', source_dir)
        target_dir = metadata['target_tablename'] + ".parquet"
        print('Target directory:', target_dir)
        df = process_file(metadata, source_dir)
        print("DataFrame loaded:")
        print(df.head())
        write_parquet(df, target_dir)
