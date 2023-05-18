import ast

import pandas as pd

from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame

import tripmining

from tripmining.model.traveler import *
from tripmining import model

from tripmining.model.countries import Countries
from tripmining.model.location import Location
from tripmining.model.trip import Trip
from tripmining.model.user import User

from multiprocessing import Pool
import re
from functools import partial


df = pd.read_json("yelp_academic_dataset_review.json", lines=True)

country_df = pd.read_csv("countries-continents.csv", sep=',')

#df = read_chuncks(chuncks)

df['text'] = df['text'].str.replace('\n', '')

## adicionando as informações do local
df_places = pd.read_json("yelp_academic_dataset_business.json", lines=True)

df_places = df_places.rename(columns={'stars': 'place_stars'})

df = df.set_index('business_id').join(df_places.set_index('business_id')).reset_index()

## adicionando as informações do usuário
df_users = pd.read_json("yelp_academic_dataset_user.json", lines=True)

#df_users = read_chuncks(user_chuncks)

df_users = df_users.rename(columns={'useful': 'user_useful',
                                    'funny': 'user_funny',
                                    'cool': 'user_cool',
                                    'name': 'user_name',
                                    'review_count': 'user_review_count'})

df = df.set_index('user_id').join(df_users.set_index('user_id')).reset_index()

df = df.reset_index()


geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]

gdf = GeoDataFrame(df, geometry=geometry)   

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

## quantidade de viagens por usuário

complete_df = gpd.sjoin(world, gdf)

users_travels = complete_df[['user_id', 'iso_a3']].drop_duplicates()

users_travels = users_travels.groupby(['user_id']).count()

users_travels = users_travels.reset_index().rename(columns={'iso_a3': 'amount_countries'})

complete_df = complete_df.set_index('user_id').join(users_travels.set_index('user_id')).reset_index()

complete_df = complete_df.rename(columns={'name_right': 'place_name', 'name_left': 'country_name'})

#complete_df = complete_df[['latitude', 'longitude', 'date', 'place_name']]

complete_df['date'] = pd.to_datetime(complete_df['date'])

country_df = country_df.rename(columns={'name': 'country_name'})[['country_name', 'country-code']]

complete_df = complete_df.rename(columns={'name_left': 'country_name'})

complete_df = complete_df.set_index('country_name').join(country_df.set_index('country_name')).reset_index()

pandas_df = complete_df.drop('geometry', axis=1)

pandas_df.to_csv("yelp_enriched_dataset.csv", sep=';', index=False)

