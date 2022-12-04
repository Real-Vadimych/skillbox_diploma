import os

from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import transform_hits
import transform_sessions





def main():

	path = os.environ.get('PROJECT_PATH', '')
	jsons_path = f'{path}data/jsons'
	sent_path = f'{path}data/jsons/sent'


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

	sessions_df = transform_sessions.pipeline(first_sessions_file)
	hits_df = transform_hits.pipeline(same_hits_file)

	try:
		load2db(df=sessions_df, target_table='sessions_test')
		os.rename(f'{jsons_path}/{sessions_file_name}',
				  f'{sent_path}/{sessions_file_name}')

		if not hits_df.empty:

			df = hits_df[hits_df.set_index(
				['session_id1', 'session_id2', 'session_id3']).index.isin(
					sessions_df.set_index(
						['session_id1', 'session_id2', 'session_id3']).index)]

			try:
				load2db(df=df, target_table='hits_test')

			except Exception as ex:
				print(ex)

		os.rename(f'{jsons_path}/{hits_file_name}',
			f'{sent_path}/{hits_file_name}')
	
	except Exception as ex:
		print(ex)

	
def pipeline() -> None:
	
	pipeline = Pipeline(steps=[
		('main', FunctionTransformer(main))
	])
	print('Success!!!')


if __name__ == '__main__':
	pipeline()
