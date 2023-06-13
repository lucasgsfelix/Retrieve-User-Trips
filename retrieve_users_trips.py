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



def identify_user_trips(user_id, max_time_without_checkin=1):
    """
        max_time_without_checkin = in days
    
    """
    
    user_df = pandas_df[(pandas_df['user_id'] == user_id)]
    
    user_city = user_df['user_visited_city'].mode().values[0]

    user_df['next_city'] = user_df['user_visited_city'].shift(-1)
    
    user_df['hometown'] = user_city
    
    # removendo as viagens feitas na cidade natal do usuário
    user_df = user_df[user_df['user_visited_city'] != user_city]
    
    user_df['date_shift'] = user_df['date'].shift(-1)

    user_df['date_diff'] = np.abs((user_df['date_shift'].dt.date - user_df['date'].dt.date).dt.days)

    user_df['date_diff'] = user_df['date_diff'].fillna(10)
    
    # agora temos para um usuário apenas as viagens que ele fez fora de sua cidade natal
    
    # reseto o index pois nós iremos trabalhar com ele
    user_df = user_df.reset_index()

    if len(user_df) == 1 or user_df['date_diff'].max() == max_time_without_checkin:
        
        user_df['trip_id'] = user_df['user_id'] + '_1'
    
    else:
        
        ## quer dizer que o usuário visitou muitos locais, 
        ## e que o tempo de diferença foi maior que 1 dia,
        ## logo nós temos variáveis viagens
        
        ## retorna o index onde o tempo sem checkin foi maior que 1
        # verificar se a próxima visita do usuário foi na sua cidade natal
        # caso seja, a viagem também acabou
        
        
        ## O INDEX QUE A VIAGEM ACABA
        index_trip_end = user_df[(user_df['date_diff'] > 1) |
                                 (user_df['next_city'] == user_city)].index

        # ci - checkin index

        user_df['trip_id'] = user_id

        trip_ids = []


        index = 0
        
        for (ci) in index_trip_end:
        

        
            if index == 0:
                
                start_index = user_df.index.min()

                end_index = ci
            
            else:
                
                start_index = end_index + 1
                
                end_index = ci

                
                
            user_df.loc[(user_df.index >= start_index) &
                        (user_df.index <= end_index), 'trip_id'] = user_id + '_' + str(index)

            index += 1


    return user_df


def mine_users_trips(user):

    try:
        
        user_trips = identify_user_trips(user)

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

pool = Pool(processes=50)

mine_function = partial(mine_users_trips)

df_trips = list(tqdm.tqdm(pool.imap(mine_function, pandas_df['user_id'].unique()), total=len(pandas_df['user_id'].unique())))


pool.close()

pool.join()

df_trips = pd.concat(list(filter(lambda x: not isinstance(x, list), df_trips))).reset_index()

if 'user_trips.csv' in os.listdir("/"):

    df_trips.to_csv("users_trips.csv", sep=';', index=False, mode='a', header=False)

else:

    df_trips.to_csv("users_trips.csv", sep=';', index=False, mode='w', header=True)


