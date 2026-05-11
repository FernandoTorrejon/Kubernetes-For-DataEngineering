from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_data(**kwargs):
    url = 'https://raw.githubusercontent.com/airscholar/ApacheFlink-SalesAnalytics/main/output/new-output.csv'
    
    # Fetch data from URL
    response = requests.get(url)
    
    if response.status_code == 200:
        
        # Read csv data
        df = pd.read_csv(url, header=None, names=['Category', 'Price', 'Quantity'])
        
        # Convert DataFrame to Json string from xcom
        json_data = df.to_json(orient='records')
        
        # Push data using xcom_push
        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data, HTTP status code: {response.status_code}')

def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')
    
    # Create DF from JSON data
    df = pd.DataFrame(output_data)
    
    # Compute total sales
    df['Total'] = df['Price'] * df['Quantity'] 
    
    # Group By (as_index False create a new index columns forech one record returned by GroupBy DML but not use Category for that information)
    df = df.groupby('Category', as_index=False).agg({'Quantity': 'sum', 'Total': 'sum'})
    
    # Sort by Total sales
    df = df.sort_values(by='Total', ascending=False)
    
    # Print first 20 rows
    print(df[['Category', 'Total']].head(20))
    
    
default_args = {
    'owner': 'datamasterylab',
    'start-date': datetime(2026, 5, 10),
    'catchup': False
}

dag = DAG(
    'fetch_and_preview',
    default_args = default_args,
    schedule=timedelta(days=1)
)
        
get_data_from_url = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)
    
preview_data_from_url = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    dag=dag
)
    
get_data_from_url >> preview_data_from_url