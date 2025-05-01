from airflow.sdk import DAG, task


from airflow.operators.python import PythonOperator


def print_hello_world():
    return 'Hello World!'

def print_random_quote():
    #print('Printing a random quote...')
    import random
    list_of_quotes = [
        "The best way to predict the future is to create it.",
        "Life is 10% what happens to us and 90 how we react to it.",
        "The only limit to our realization of tomorrow will be our doubts of today.",
        "Success is not the key to happiness. Happiness is the key to success."
    ]

    quote = random.choice(list_of_quotes)

    return 'Quote of the day: "{}"'.format(quote)

def read_csv_file():
    import pandas as pd
    import os
    print('Reading CSV file...')

    absolute_path = os.path.abspath(__file__)
    directory_path = os.path.dirname(absolute_path)
    csv_path = os.path.join(directory_path, '../data/atp2024.csv')
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Print the DataFrame
    print(df.head())

    df['modified'] = 1
    df.to_csv(os.path.join(directory_path, '../data/modified.csv'), index=False)



dag = DAG(

    'new_dag',

    #default_args={'start_date': str(datetime.today().date())},

    #schedule_interval='0 23 * * *',

    #catchup=False

) 
hello = PythonOperator(
    task_id='hello_world',
    python_callable=print_hello_world,
    dag=dag
)

@task(dag=dag)
def print_random_quote_task():
    print(print_random_quote())

@task(dag=dag)
def print_hello_world_task():
    print(print_hello_world())

hello

# print_random_quote_task = PythonOperator(
#     task_id='print_random_quote',

#     python_callable=print_random_quote,

#     dag=dag

# )

# database_task = PythonOperator(
#     task_id='database_task',
#     python_callable=read_csv_file,
#     dag=dag
# )

#Set the dependencies between the tasks

#print_random_quote_task >> database_task