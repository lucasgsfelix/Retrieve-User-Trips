import pandas as pd

import tqdm

import os


if __name__ == '__main__':


	pandas_df = pd.read_csv("yelp_enriched_dataset.csv", sep=';')

	print("Quantidade de linhas antes do dropnat: ", len(pandas_df))

	pandas_df['date'] = pd.to_datetime(pandas_df['date'], errors='coerce')

	pandas_df = pandas_df.dropna(subset='date')

	print("Quantidade de linhas depois do dropnat: ", len(pandas_df))


	batches = np.array_split(pandas_df['user_id'].unique(), 100)

	for users in tqdm.tqdm(batches):

		pandas_df[pandas_df['user_id'].isin(users)].to_csv("yelp_users_batches.csv", sep=';', index=False)

		os.system("retrieve_users_trips.py")


