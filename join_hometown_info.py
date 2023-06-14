
## join hometown info

import pandas as pd


if __name__ == '__main__':

	df = pd.read_table("geolocation_user_hometown.csv", sep=';')

	df = df.rename(columns={'latitude': 'home_lat', 'longitude': 'home_lon', 'user_visited_city': 'hometown'})

	df_trips = pd.read_table("user_trips_table.csv", sep=';')

	df = df_trips.set_index('hometown').join(df.set_index('hometown')).reset_index()

	df.to_csv("user_trips_table_plus_home_geo.csv", sep=';', index=False)
