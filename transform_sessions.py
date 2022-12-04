import pandas as pd
import json
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
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


def change_type(df: pd.DataFrame) -> pd.DataFrame:
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



def create_keys(df: pd.DataFrame) -> pd.DataFrame:
	dfa = pd.DataFrame()
	dfa[['session_id1', 'session_id2', 'session_id3']] = [pd.to_numeric(i.split('.')) for i in df.session_id]
	df[['session_id1', 'session_id2', 'session_id3']] = dfa[['session_id1', 'session_id2', 'session_id3']]
	df = df.drop('session_id', axis=1)

	return df

def pipeline(json_file_path) -> None:
	with open (json_file_path, 'rb') as file:
		data = json.load(file)
	df = pd.DataFrame(list(data.values())[0])
	
	if df.empty:
		return df

	pipeline = Pipeline(steps=[
		('drop_columns', FunctionTransformer(drop_columns)),
		('change_types', FunctionTransformer(change_type)),
		('keys_creator', FunctionTransformer(create_keys))
	])

	return pipeline.fit_transform(df)


if __name__ == '__main__':
	pipeline('data/jsons/ga_sessions_new_2022-01-01.json')

