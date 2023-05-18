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

    
    user_obj, user_df = define_user_object(user)
    
    user_trips = define_user_trips(user_obj, user_df)

    return user_trips


def read_chuncks(chuncks):
    
    for chunck in chuncks:
    
    
        #chunck.to_csv("reviews_chunck.csv", sep=';', index=False)

        break
    
    return chunck


if __name__ == '__main__':

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

    pool = Pool(processes=16)

    pandas_df = complete_df.drop('geometry', axis=1)

    global pandas_df

    mine_function = partial(mine_users_trips)

    df_trips = pool.map(mine_function, pandas_df['user_id'].unique())

    df_trips = pd.concat(list(filter(lambda x: not isinstance(x, list), df_trips))).reset_index()

    df_trips.to_csv("users_trips.csv", sep=';', index=False)

