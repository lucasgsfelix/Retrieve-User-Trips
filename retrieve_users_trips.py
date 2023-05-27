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
import tqdm

import os



def define_user_object(user_id):

    user_df = pandas_df[pandas_df['user_id'] == user_id]

    users_checkins = []

    selected_columns = ['business_id', 'latitude', 'longitude', 'date', 'place_name', 'country-code']

    for bis, lat, lon, date, place_name, code in user_df[selected_columns].values:

        #latitude, longitude
        coordinates_obj = model.coordinate.Coordinate(lat, lon)

        location_obj = model.location.Location(place_name, coordinates_obj, code, name=place_name)

        # user_id, cooridnates_obj, visitt_datetime
        checkins = model.checkin.Checkin(user_id, location_obj, date)

        users_checkins.append(checkins)

    user = model.user.User(user_id, users_checkins, '') #  user_id, checkins_obj, dataset

    return user, user_df


def define_user_trips(user, user_df):

    traveler = Traveler(user)

    user_trips = []

    for trip in traveler.trips:

        trip_json = ast.literal_eval(trip.to_json())

        blocks_df = pd.DataFrame(trip_json['blocks'])

        trasitions_df = pd.DataFrame(trip_json['transitions'])

        trip_df = pd.json_normalize(trip_json)

        trip_df = trip_df.drop(['blocks', 'transitions'], axis=1)

        ## just a joining id
        trip_df['join_index'] = 1

        trasitions_df['join_index'] = 1

        transitions_df = trasitions_df.set_index('join_index')

        trip_df = trip_df.set_index('join_index').join(transitions_df, lsuffix='_transition').reset_index(drop=True)

        trip_df = trip_df.join(blocks_df, lsuffix='_blocks')

        user_trip_df = user_df.set_index(['place_name']).join(trip_df.set_index('location_id'), how='right')

        user_trips.append(user_trip_df)
    
    if user_trips:
    
        return pd.concat(user_trips)

    return []


def mine_users_trips(user):

    try:
        user_obj, user_df = define_user_object(user)
    
        user_trips = define_user_trips(user_obj, user_df)

        del user_obj, user_df

    except:

        user_trips = []

    return user_trips


def read_chuncks(chuncks):
    
    for chunck in chuncks:
    
    
        #chunck.to_csv("reviews_chunck.csv", sep=';', index=False)

        break
    
    return chunck

pandas_df = pd.read_csv("yelp_users_batches.csv", sep=';')

print("Processamento dos dados!")

pool = Pool(processes=40)

mine_function = partial(mine_users_trips)

df_trips = list(tqdm.tqdm(pool.imap(mine_function, pandas_df['user_id'].unique()), total=len(pandas_df['user_id'].unique())))


pool.close()

pool.join()

df_trips = pd.concat(list(filter(lambda x: not isinstance(x, list), df_trips))).reset_index()

if 'user_trips.csv' in os.listdir("/"):

    df_trips.to_csv("users_trips.csv", sep=';', index=False, mode='a', header=False)

else:

    df_trips.to_csv("users_trips.csv", sep=';', index=False, mode='w', header=True)


