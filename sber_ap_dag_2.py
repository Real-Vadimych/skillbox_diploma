import datetime as dt
import json
import os
import sys
from datetime import timedelta

import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sqlalchemy import create_engine

path = os.path.expanduser('/home/vvk/DataGripProjects/skillbox_diploma')
os.environ['PROJECT_PATH'] = path

sys.path.insert(0, path)


def h_drop_columns(df: pd.DataFrame) -> pd.DataFrame:
	drop_col = [
		'event_value', 
		'hit_time', 
		'hit_referer', 
		'event_label'
		]
	
	df = df.drop(drop_col, axis=1)

	return df

def h_change_type(df: pd.DataFrame) -> pd.DataFrame:
	cat_columns = [
    	'hit_type',
    	'event_category',
    	'event_action'
     	]

	for column in cat_columns:
		df[column] = df[column].astype('category')

	df.hit_date = df.hit_date.astype('datetime64')

	return df

def h_create_features(df: pd.DataFrame) -> pd.DataFrame:
	df['car_manufacturer'] = df.hit_page_path.apply(
		lambda x: 
		x.split('sberauto.com/cars/all/')[1].split('/')[0] 
		if x.startswith('sberauto.com/cars/all/') 
		else "")

	df['car_model'] = df.hit_page_path.apply(
		lambda x: 
		x.split('sberauto.com/cars/all/')[1].split('/')[1] 
		if x.startswith('sberauto.com/cars/all/') 
		else "")
	df = df.drop('hit_page_path', axis=1)

	return df

def h_create_keys(df: pd.DataFrame) -> pd.DataFrame:
	dfa = pd.DataFrame()
	dfa[['session_id1', 'session_id2', 'session_id3']] = [pd.to_numeric(i.split('.')) for i in df.session_id]
	df[['session_id1', 'session_id2', 'session_id3']] = dfa[['session_id1', 'session_id2', 'session_id3']]
	df = df.drop('session_id', axis=1)

	return df


def h_pipeline(json_file_path) -> None:
	with open (json_file_path, 'rb') as file:
		data = json.load(file)
	df = pd.DataFrame(list(data.values())[0])
	
	if df.empty:
		return df

	pipeline = Pipeline(steps=[
		('drop_columns', FunctionTransformer(h_drop_columns)),
		('change_types', FunctionTransformer(h_change_type)),
		('feature_creator', FunctionTransformer(h_create_features)),
		('keys_creator', FunctionTransformer(h_create_keys))
	])


	return pipeline.fit_transform(df)

# ======
def s_drop_columns(df: pd.DataFrame) -> pd.DataFrame:
	drop_col = [
		'device_model',
		'utm_keyword',
		'device_os',
		'utm_adcontent',
		'utm_campaign',
		'device_brand',
		'client_id'
		]
	df = df.drop(drop_col, axis=1)
	return df


def s_change_type(df: pd.DataFrame) -> pd.DataFrame:
	cat_columns = [
		'utm_source',
		'utm_medium',
		'device_category',
		'device_screen_resolution',
		'device_browser',
		'geo_country',
		'geo_city'
		]

	for column in cat_columns:
		df[column] = df[column].astype('category')
	
	df.visit_number = df.visit_number.astype('int16')
	df.visit_date = df.visit_date.astype('datetime64')

	return df



def s_create_keys(df: pd.DataFrame) -> pd.DataFrame:
	dfa = pd.DataFrame()
	dfa[['session_id1', 'session_id2', 'session_id3']] = [pd.to_numeric(i.split('.')) for i in df.session_id]
	df[['session_id1', 'session_id2', 'session_id3']] = dfa[['session_id1', 'session_id2', 'session_id3']]
	df = df.drop('session_id', axis=1)

	return df

def s_pipeline(json_file_path) -> None:
	with open (json_file_path, 'rb') as file:
		data = json.load(file)
	df = pd.DataFrame(list(data.values())[0])
	
	if df.empty:
		return df

	pipeline = Pipeline(steps=[
		('drop_columns', FunctionTransformer(s_drop_columns)),
		('change_types', FunctionTransformer(s_change_type)),
		('keys_creator', FunctionTransformer(s_create_keys))
	])

	return pipeline.fit_transform(df)






def main():

	path = os.environ.get('PROJECT_PATH', '')
	jsons_path = f'{path}/data/jsons'
	sent_path = f'{path}/data/jsons/sent'


	def load2db(df, target_table):

		conn_string = 'postgresql://datagrip:datagrip@localhost:5432/diploma_db'
		engine = create_engine(conn_string)

		df.to_sql(target_table,
				engine,
				schema='dbo',
				if_exists='append',
				index=False,
				chunksize=100000,
				method='multi')


	def file_date(x):
		return x[-15:]

	list_of_sessions_files = [
		i for i in os.listdir(jsons_path) if 'sessions' in i
	]

	list_of_hits_files = [i for i in os.listdir(jsons_path) if 'hits' in i]

	first_sessions_file = f'{jsons_path}/{sorted(list_of_sessions_files, key=file_date)[0]}'
	same_hits_file = f'{jsons_path}/{[i for i in list_of_hits_files if first_sessions_file[-15:] in i][0]}'

	sessions_file_name = os.path.basename(first_sessions_file)
	hits_file_name = os.path.basename(same_hits_file)

	sessions_df = s_pipeline(first_sessions_file)
	hits_df = h_pipeline(same_hits_file)

	try:
		load2db(df=sessions_df, target_table='sessions')
		os.rename(f'{jsons_path}/{sessions_file_name}',
				  f'{sent_path}/{sessions_file_name}')

		if not hits_df.empty:

			df = hits_df[hits_df.set_index(
				['session_id1', 'session_id2', 'session_id3']).index.isin(
					sessions_df.set_index(
						['session_id1', 'session_id2', 'session_id3']).index)]

			try:
				load2db(df=df, target_table='hits')

			except Exception as ex:
				print(ex)

		os.rename(f'{jsons_path}/{hits_file_name}',
			f'{sent_path}/{hits_file_name}')
	
	except Exception as ex:
		print(ex)

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 1, 29),
    'retries': 10,
    'retry_delay': dt.timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
        dag_id='sber_autopodpiska_json_2_db',
        schedule_interval="0 * * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=main,
    )

    pipeline