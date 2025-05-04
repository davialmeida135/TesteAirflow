from airflow.sdk import DAG, task
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import os
absolute_path = os.path.abspath(__file__)
directory_path = os.path.dirname(absolute_path)

def read_csv_file(**context):
    import pandas as pd
    print('Reading CSV file...')

    csv_path = os.path.join(directory_path, 'data/raw/atp2023.csv')
    df = pd.read_csv(csv_path)

    print(df.head())

    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def drop_cols(**context):
    """Remove unnecessary columns"""
    import pandas as pd
    print('Finalizing data...')
    df_json = context["task_instance"].xcom_pull(task_ids="extract_task", key="tennis_df")
    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    df = df.drop(columns=['w_ace', 'w_df','w_svpt','w_1stIn','w_1stWon',
                     'w_2ndWon','w_SvGms','w_bpSaved','w_bpFaced',
                     'l_ace','l_df','l_svpt','l_1stIn','l_1stWon',
                     'l_2ndWon', 'l_SvGms','l_bpSaved','l_bpFaced',
                      'winner_ioc','loser_ioc','tourney_name','minutes'
                     ])
    
    df.to_csv(os.path.join(directory_path, 'data/processed/dropped_cols.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def fill_missing(**context):
    import pandas as pd
    print('Filling missing values...')
    df_json = context["task_instance"].xcom_pull(task_ids="drop_cols_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    df['surface'] = df['surface'].fillna('Hard')
    df['winner_ht'] = df['winner_ht'].fillna(df['winner_ht'].mean())
    df['loser_ht'] = df['loser_ht'].fillna(df['loser_ht'].mean())
    df['winner_age'] = df['winner_age'].fillna(df['winner_age'].mean())
    df['loser_age'] = df['loser_age'].fillna(df['loser_age'].mean())
    df['winner_rank'] = df['winner_rank'].fillna(df['winner_rank'].mean())
    df['loser_rank'] = df['loser_rank'].fillna(df['loser_rank'].mean())

    df['winner_rank_points'] = df['winner_rank_points'].fillna(df['winner_rank_points'].min())
    df['loser_rank_points'] = df['loser_rank_points'].fillna(df['loser_rank_points'].min())

    df.to_csv(os.path.join(directory_path, 'data/processed/filled.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def transform_seed_data(**context):
    import pandas as pd
    print("Transforming seed data")
    # Create copy to avoid warnings
    df_json = context["task_instance"].xcom_pull(task_ids="drop_cols_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    result = pd.read_json(df_json)

    result['winner_seed_value'] = pd.to_numeric(result['winner_seed'], errors='coerce')
    result['loser_seed_value'] = pd.to_numeric(result['loser_seed'], errors='coerce')
    result['winner_seeded'] = False
    result['loser_seeded'] = False
    result['winner_unseeded'] = False
    result['loser_unseeded'] = False
    result['winner_qualifier'] = False
    result['loser_qualifier'] = False
    result['winner_lucky_loser'] = False
    result['loser_lucky_loser'] = False
    result['winner_special_exempt'] = False
    result['loser_special_exempt'] = False
    result['winner_alternate'] = False
    result['loser_alternate'] = False
    result['winner_wildcard'] = False
    result['loser_wildcard'] = False
    result['winner_protected_ranking'] = False
    result['loser_protected_ranking'] = False

    winner_entry_methods = {'S':'winner_seeded', 
                            'US':'winner_unseeded', 
                            'WC':'winner_wildcard',
                            'Q':'winner_qualifier', 
                            'LL':'winner_lucky_loser',
                            'PR':'winner_protected_ranking',
                            'SE':'winner_special_exempt',
                            'ALT':'winner_alternate'}
    
    loser_entry_methods = {'S':'loser_seeded',
                            'US':'loser_unseeded', 
                            'WC':'loser_wildcard',
                            'Q':'loser_qualifier', 
                            'LL':'loser_lucky_loser',
                            'PR':'loser_protected_ranking',
                            'SE':'loser_special_exempt',
                            'ALT':'loser_alternate'}
    
    for index, row in result.iterrows():
        if str(row['winner_seed_value'])[0].isdigit():
            result.at[index, 'winner_seeded'] = True
        elif pd.isna(row['winner_seed_value']) and pd.isna(row['winner_entry']):
            result.at[index, 'winner_unseeded'] = True
            result.at[index, 'winner_seed_value'] = row['draw_size']
        else:
            result.at[index, winner_entry_methods.get(row['winner_entry'].upper())] = True
            result.at[index, 'winner_seed_value'] = row['draw_size']

        if str(row['loser_seed_value'])[0].isdigit():
            result.at[index, 'loser_seeded'] = True
        elif pd.isna(row['loser_seed_value']) and pd.isna(row['loser_entry']): 
            result.at[index, 'loser_unseeded'] = True
            result.at[index, 'loser_seed_value'] = row['draw_size']
        else:
            result.at[index,loser_entry_methods.get(row['loser_entry'].upper())] = True
            result.at[index, 'loser_seed_value'] = row['draw_size']
   
    # Drop original columns
    result = result.drop(columns=['winner_seed', 'loser_seed', 'winner_entry', 'loser_entry'])
    result['winner_seed_value'] = result['winner_seed_value'].astype('Int64',errors='ignore')
    result['loser_seed_value'] = result['loser_seed_value'].astype('Int64',errors='ignore')
    
    print("Seed data transformed")
    result.to_csv(os.path.join(directory_path, 'data/processed/transformed_seed.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=result.to_json())

def encode_surface(**context):
    import pandas as pd
    from sklearn.preprocessing import OneHotEncoder
    print('Applying one-hot encoding...')
    df_json = context["task_instance"].xcom_pull(task_ids="drop_cols_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    encoder = OneHotEncoder(sparse_output=False,)
    encoded_surface = encoder.fit_transform(df[['surface']])
    encoded_surface_df = pd.DataFrame(encoded_surface, columns=encoder.get_feature_names_out(['surface']))
    df = df.join(encoded_surface_df)

    df.to_csv(os.path.join(directory_path, 'data/processed/encoded.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def preprocess_dates(**context):
    import pandas as pd
    print('Processing dates...')
    df_json = context["task_instance"].xcom_pull(task_ids="drop_cols_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    """Convert integer dates to datetime format"""
    df['tourney_date'] = pd.to_datetime(df['tourney_date'].astype(str), format='%Y%m%d')
    df['week'] = df['tourney_date'].dt.isocalendar().week

    df.to_csv(os.path.join(directory_path, 'data/processed/parsed_dates.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def merge_data(**context):
    """Combines polynomial and encoded features into a single DataFrame."""
    import pandas as pd
    # Pull from the correct upstream tasks based on the desired logic
    encoded_json = context["task_instance"].xcom_pull(task_ids="surface_task", key="tennis_df")
    dates_json = context["task_instance"].xcom_pull(task_ids="parse_dates_task", key="tennis_df")
    filled_json = context["task_instance"].xcom_pull(task_ids="fill_missing_task", key="tennis_df")
    seeded_json = context["task_instance"].xcom_pull(task_ids="transform_seed_task", key="tennis_df")

    if not encoded_json or not dates_json or not filled_json or not seeded_json:
        raise ValueError("Could not retrieve features from XCom.")

    df_encoded = pd.read_json(encoded_json)
    df_dates = pd.read_json(dates_json)
    df_filled = pd.read_json(filled_json)
    df_seeded = pd.read_json(seeded_json)

    df_combined = df_filled.copy()

    if 'week' in df_dates.columns:
        df_combined['week'] = df_dates['week']

    else:
        print("Warning: 'week' not found in df_dates XCom.")

    new_columns = df_encoded.columns.difference(df_combined.columns, sort=False)
    # Merge the encoded DataFrame with the filled DataFrame
    df_new_columns = df_encoded[new_columns]
    df_combined = df_combined.join(df_new_columns)



    new_columns = df_seeded.columns.difference(df_combined.columns, sort=False)
    # Merge the encoded DataFrame with the filled DataFrame
    df_new_columns = df_seeded[new_columns]
    df_combined = df_combined.join(df_new_columns)

    # Verify no duplicate columns before saving/pushing
    if df_combined.columns.duplicated().any():
        duplicates = df_combined.columns[df_combined.columns.duplicated()].tolist()
        print(f"Duplicate columns found after merge: {duplicates}")
        # Raise an error to fail the task if duplicates exist
        raise ValueError(f"Duplicate columns detected after merge: {duplicates}")

    df_combined.to_csv(os.path.join(directory_path, 'data/processed/combined.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df_combined.to_json())
    print("Combined features DataFrame head:")
    print(df_combined.head())

def sort_data(**context):
    import pandas as pd
    import os
    print('Sorting data...')
    df_json = context["task_instance"].xcom_pull(task_ids="combine_task", key="tennis_df")

    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    """Sort the DataFrame by tournament date, tournament ID, and match number"""
    df = df.sort_values(by=['tourney_date','tourney_id','match_num'])
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())
    df.to_csv(os.path.join(directory_path, 'data/processed/sorted.csv'), index=False)


def fix_categoric_data(**context):
    import pandas as pd
    df_json = context["task_instance"].xcom_pull(task_ids="sort_data_task", key="tennis_df")
    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    print("Removing Walkovers")
    df = df[df['score'] != 'W/O']

    """Transform tournament level to categorical values"""
    dictionary = {'D':0,'A':1,'M':2,'G':3,'F':4,}
    df['tourney_level'] = df['tourney_level'].apply(lambda x: dictionary.get(x, 0))

    """Transform handedness to categorical values"""
    df['winner_hand'] = df['winner_hand'].apply(lambda x: 0 if x == 'R' else 1)
    df['loser_hand'] = df['loser_hand'].apply(lambda x: 0 if x == 'R' else 1)

    """Transform round to categorical values"""
    dictionary = {'F':0,'SF':1,'QF':2,'R16':3,'R32':4,'R64':5,'R128':6, 'RR':3} # Talvez one hot melhor
    df['round'] = df['round'].apply(lambda x: dictionary.get(x, 0))

    df.to_csv(os.path.join(directory_path, 'data/processed/categoric.csv'), index=False)
    context["task_instance"].xcom_push(key="tennis_df", value=df.to_json())

def anonymize(**context):
    """
    Troca os nomes dos jogadores por player1 e player2"
    """
    import pandas as pd
    import numpy as np
    print('Finalizing data...')
    df_json = context["task_instance"].xcom_pull(task_ids="fix_categoric_data_task", key="tennis_df")
    if not df_json:
        raise ValueError("Could not retrieve tennis_df from XCom.")
    df = pd.read_json(df_json)

    rows_list = []

    # Iterar sobre as linhas do DataFrame trocando loser por player1 e winner por player2
    # Fazer de maneira aleatória
    for index, row in df.iterrows():
        if np.random.randint(0, 2) == 0:
            rows_list.append({
                'draw_size': row['draw_size'],
                'tourney_level': row['tourney_level'],
                'tourney_date': row['tourney_date'],
                'match_num': row['match_num'],
                'player0_hand': row['loser_hand'],
                'player0_ht': row['loser_ht'],
                'player0_age': row['loser_age'],
                'player0_rank': row['loser_rank'],
                'player0_rank_points': row['loser_rank_points'],
                'player1_hand': row['winner_hand'],
                'player1_ht': row['winner_ht'],
                'player1_age': row['winner_age'],
                'player1_rank': row['winner_rank'],
                'player1_rank_points': row['winner_rank_points'],
                'best_of': row['best_of'],
                'round': row['round'],
                'player0_seed_value': row['loser_seed_value'],
                'player1_seed_value': row['winner_seed_value'],
                'player0_seeded': row['loser_seeded'],
                'player1_seeded': row['winner_seeded'],
                'player0_unseeded': row['loser_unseeded'],
                'player1_unseeded': row['winner_unseeded'],
                'player0_qualifier': row['loser_qualifier'],
                'player1_qualifier': row['winner_qualifier'],
                'player0_lucky_loser': row['loser_lucky_loser'],
                'player1_lucky_loser': row['winner_lucky_loser'],
                'player0_special_exempt': row['loser_special_exempt'],
                'player1_special_exempt': row['winner_special_exempt'],
                'player0_alternate': row['loser_alternate'],
                'player1_alternate': row['winner_alternate'],
                'player0_wildcard': row['loser_wildcard'],
                'player1_wildcard': row['winner_wildcard'],
                'player0_protected_ranking': row['loser_protected_ranking'],
                'player1_protected_ranking': row['winner_protected_ranking'],
                'surface_Hard': row['surface_Hard'],
                'surface_Clay': row['surface_Clay'],
                'surface_Grass': row['surface_Grass'],
                'winner': 1,
            })
        else:
            rows_list.append({
                'draw_size': row['draw_size'],
                'tourney_level': row['tourney_level'],
                'tourney_date': row['tourney_date'],
                'match_num': row['match_num'],
                'player1_hand': row['loser_hand'],
                'player1_ht': row['loser_ht'],
                'player1_age': row['loser_age'],
                'player1_rank': row['loser_rank'],
                'player1_rank_points': row['loser_rank_points'],
                'player0_hand': row['winner_hand'],
                'player0_ht': row['winner_ht'],
                'player0_age': row['winner_age'],
                'player0_rank': row['winner_rank'],
                'player0_rank_points': row['winner_rank_points'],
                'best_of': row['best_of'],
                'round': row['round'],
                'player0_seed_value': row['winner_seed_value'],
                'player1_seed_value': row['loser_seed_value'],
                'player0_seeded': row['winner_seeded'],
                'player1_seeded': row['loser_seeded'],
                'player0_unseeded': row['winner_unseeded'],
                'player1_unseeded': row['loser_unseeded'],
                'player0_qualifier': row['winner_qualifier'],
                'player1_qualifier': row['loser_qualifier'],
                'player0_lucky_loser': row['winner_lucky_loser'],
                'player1_lucky_loser': row['loser_lucky_loser'],
                'player0_special_exempt': row['winner_special_exempt'],
                'player1_special_exempt': row['loser_special_exempt'],
                'player0_alternate': row['winner_alternate'],
                'player1_alternate': row['loser_alternate'],
                'player0_wildcard': row['winner_wildcard'],
                'player1_wildcard': row['loser_wildcard'],
                'player0_protected_ranking': row['winner_protected_ranking'],
                'player1_protected_ranking': row['loser_protected_ranking'],
                'surface_Hard': row['surface_Hard'],
                'surface_Clay': row['surface_Clay'],
                'surface_Grass': row['surface_Grass'],
                'winner': 0,
            })

    anon_df = pd.DataFrame(rows_list)
    anon_df.to_csv(os.path.join(directory_path, 'data/processed/final.csv'), index=False)
    #context["task_instance"].xcom_push(key="tennis_df", value=anon_df.to_json())

dag = DAG(
    'tennis_dag',
    schedule='0 23 * * *',
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=read_csv_file,
    dag=dag
)

drop_cols_task = PythonOperator(
    task_id='drop_cols_task',
    python_callable=drop_cols,
    dag=dag
)

transform_seed_task = PythonOperator(
    task_id='transform_seed_task',
    python_callable=transform_seed_data,
    dag=dag
)

fill_missing_task = PythonOperator(
    task_id='fill_missing_task',
    python_callable=fill_missing,
    dag=dag
)

surface_task = PythonOperator(
    task_id='surface_task',
    python_callable=encode_surface,
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

fix_categoric_data_task = PythonOperator(
    task_id='fix_categoric_data_task',
    python_callable=fix_categoric_data,
    dag=dag
)

anonymize_task = PythonOperator(
    task_id='anonymize_task',
    python_callable=anonymize,
    dag=dag
)

extract_task >> drop_cols_task
drop_cols_task >> [fill_missing_task, surface_task, parse_dates_task, transform_seed_task]
[fill_missing_task, surface_task, parse_dates_task, transform_seed_task] >> combine_task
combine_task >> sort_data_task
sort_data_task >> fix_categoric_data_task
fix_categoric_data_task >> anonymize_task