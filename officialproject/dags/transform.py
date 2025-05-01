from airflow.sdk import DAG, task
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import os
absolute_path = os.path.abspath(__file__)
directory_path = os.path.dirname(absolute_path)

def read_csv_file(**context):
    import pandas as pd
    print('Reading CSV file...')

    csv_path = os.path.join(directory_path, 'data/atp2023.csv')
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Print the DataFrame
    print(df.head())

    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def fill_missing(**context):
    import pandas as pd
    print('Filling missing values...')
    df_json = context["task_instance"].xcom_pull(task_ids="extract_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve weather_df from XCom.")
    df = pd.read_json(df_json)

    df['surface'] = df['surface'].fillna('Hard')
    df['winner_ht'] = df['winner_ht'].fillna(df['winner_ht'].mean())
    df['loser_ht'] = df['loser_ht'].fillna(df['loser_ht'].mean())
    df['winner_age'] = df['winner_age'].fillna(df['winner_age'].mean())
    df['loser_age'] = df['loser_age'].fillna(df['loser_age'].mean())
    df['winner_rank'] = df['winner_rank'].fillna(df['winner_rank'].max())
    df['loser_rank'] = df['loser_rank'].fillna(df['loser_rank'].max())

    df['winner_rank_points'] = df['winner_rank_points'].fillna(df['winner_rank_points'].min())
    df['loser_rank_points'] = df['loser_rank_points'].fillna(df['loser_rank_points'].min())

    df.to_csv(os.path.join(directory_path, 'data/filled.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def apply_one_hot_encoding(**context):
    import pandas as pd
    from sklearn.preprocessing import OneHotEncoder
    print('Applying one-hot encoding...')
    df_json = context["task_instance"].xcom_pull(task_ids="fill_missing_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve weather_df from XCom.")
    df = pd.read_json(df_json)

    # Apply one-hot encoding to the 'surface' column
    encoder = OneHotEncoder(sparse_output=False,)
    encoded_surface = encoder.fit_transform(df[['surface']])
    encoded_surface_df = pd.DataFrame(encoded_surface, columns=encoder.get_feature_names_out(['surface']))
    df = df.join(encoded_surface_df)

    df.to_csv(os.path.join(directory_path, 'data/encoded.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def preprocess_dates(**context):
    import pandas as pd
    print('Processing dates...')
    df_json = context["task_instance"].xcom_pull(task_ids="fill_missing_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve weather_df from XCom.")
    df = pd.read_json(df_json)

    """Convert integer dates to datetime format"""
    df['tourney_date'] = pd.to_datetime(df['tourney_date'].astype(str), format='%Y%m%d')
    df['week'] = df['tourney_date'].dt.isocalendar().week

    df.to_csv(os.path.join(directory_path, 'data/parsed_dates.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def merge_data(**context):
    """Combines polynomial and encoded features into a single DataFrame."""
    import pandas as pd
    # Pull from the correct upstream tasks based on the desired logic
    encoded_json = context["task_instance"].xcom_pull(task_ids="encode_task", key="tennis_df")
    dates_json = context["task_instance"].xcom_pull(task_ids="parse_dates_task", key="tennis_df")

    if not encoded_json or not dates_json:
        raise ValueError("Could not retrieve encoded or date features from XCom.")

    df_encoded = pd.read_json(encoded_json)
    df_dates = pd.read_json(dates_json)

    df_combined = df_encoded.copy()

    if 'week' in df_dates.columns:
        df_combined['week'] = df_dates['week']
    else:
        print("Warning: 'week' not found in df_dates XCom.")

    # Verify no duplicate columns before saving/pushing
    if df_combined.columns.duplicated().any():
        duplicates = df_combined.columns[df_combined.columns.duplicated()].tolist()
        print(f"Duplicate columns found after merge: {duplicates}")
        # Raise an error to fail the task if duplicates exist
        raise ValueError(f"Duplicate columns detected after merge: {duplicates}")

    df_combined.to_csv(os.path.join(directory_path, 'data/combined.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df_combined.to_json())
    print("Combined features DataFrame head:")
    print(df_combined.head())

def sort_data(**context):
    import pandas as pd
    import os
    print('Sorting data...')
    df_json = context["task_instance"].xcom_pull(task_ids="combine_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve weather_df from XCom.")
    df = pd.read_json(df_json)

    """Sort the DataFrame by tournament date, tournament ID, and match number"""
    df = df.sort_values(by=['tourney_date','tourney_id','match_num'])
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())
    df.to_csv(os.path.join(directory_path, 'data/sorted.csv'), index=False)

dag = DAG(
    'tennis_dag',
    schedule='0 23 * * *',
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=read_csv_file,
    dag=dag
)

fill_missing_task = PythonOperator(
    task_id='fill_missing_task',
    python_callable=fill_missing,
    dag=dag
)

encode_task = PythonOperator(
    task_id='encode_task',
    python_callable=apply_one_hot_encoding,
    dag=dag
)

parse_dates_task = PythonOperator(
    task_id='parse_dates_task',
    python_callable=preprocess_dates,
    dag=dag
)

combine_task = PythonOperator(
    task_id='combine_task',
    python_callable=merge_data,
    dag=dag
)

sort_data_task = PythonOperator(
    task_id='sort_data_task',
    python_callable=sort_data,
    dag=dag
)

extract_task >> fill_missing_task
fill_missing_task>>[encode_task, parse_dates_task]
[encode_task, parse_dates_task] >> combine_task
combine_task >> sort_data_task