import pandas as pd
import json
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
	drop_col = [
		'event_value', 
		'hit_time', 
		'hit_referer', 
		'event_label'
		]
	
	df = df.drop(drop_col, axis=1)

	return df

def change_type(df: pd.DataFrame) -> pd.DataFrame:
	cat_columns = [
    	'hit_type',
    	'event_category',
    	'event_action'
     	]

	for column in cat_columns:
		df[column] = df[column].astype('category')

	df.hit_date = df.hit_date.astype('datetime64')

	return df

def create_features(df: pd.DataFrame) -> pd.DataFrame:
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
		('feature_creator', FunctionTransformer(create_features)),
		('keys_creator', FunctionTransformer(create_keys))
	])


	return pipeline.fit_transform(df)


if __name__ == '__main__':
	pipeline('data/jsons/ga_hits_new_2022-01-01.json')

	

