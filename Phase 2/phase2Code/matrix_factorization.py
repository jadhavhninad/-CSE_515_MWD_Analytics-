from mysqlConn import DbConnect
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
import argparse
import operator
from math import log,exp
import pprint


#DB connector and curosor
db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

#Argument parser
'''
parser = argparse.ArgumentParser()
parser.add_argument("USER")
args = parser.parse_args()
'''

#===========================================================================
#Generate user - movie_rating matrix.
#For each movie, get its rating given by a user. If no rating then give zero
#===========================================================================

dd_users_mvrating = {}
dd_av_rating_for_genre = {}
dd_total_movie_for_genre = {}

cur2.execute("SELECT userid FROM `mlusers` limit 200")
result0 = cur2.fetchall();
for usr in result0:
    print "for user" , usr[0]
    dd_users_mvrating[usr[0]] = {}
    dd_av_rating_for_genre[usr[0]] = {}
    dd_total_movie_for_genre[usr[0]] = {}

    #Get all movies watched(and hence rated) by each user.
    cur2.execute("SELECT movieid, rating FROM `mlratings` where userid = %s",usr)
    result1 = cur2.fetchall()
    for data1 in result1:

        user_movie_id = data1[0]
        user_movie_rating = data1[1]

        if user_movie_id in dd_users_mvrating[usr[0]]:
            continue
        else:
            print user_movie_id,user_movie_rating
            dd_users_mvrating[usr[0]][user_movie_id] = user_movie_rating

        cur2.execute("SELECT genres FROM `mlmovies_clean` where movieid = %s", {user_movie_id,})
        result_gen = cur2.fetchone()
        for data in result_gen:
            if data[0] in dd_av_rating_for_genre[usr[0]]:
                dd_av_rating_for_genre[usr[0]][data[0]] += user_movie_rating
                dd_total_movie_for_genre[usr[0]][data[0]] += 1
            else:
                dd_av_rating_for_genre[usr[0]][data[0]] = user_movie_rating;
                dd_total_movie_for_genre[usr[0]][data[0]] = 1

    #WE need to do this again for mltags because it does not have a rating,
    # give rating = avg rating give to a particular genre to by a user.
    print "Getting mltags data........."
    # Get all movies tagged by each user. If movie is only tagged and not rated, then give rating of 2 (avg).
    cur2.execute("SELECT movieid FROM `mltags` where userid = %s", usr)
    result2 = cur2.fetchall()
    for data in result2:
        #print data1
        user_movie_id = data[0]
        cur2.execute("SELECT genres FROM `mlmovies_clean` where movieid = %s", {user_movie_id, })
        mv_genre = cur2.fetchone()[0]

        if user_movie_id in dd_users_mvrating[usr[0]]:
            continue
        else:
            print user_movie_id
            dd_users_mvrating[usr[0]][user_movie_id] = dd_av_rating_for_genre[usr[0]][mv_genre]/dd_total_movie_for_genre[usr[0]][mv_genre]


    #Make rating of other movies to zero.
    cur2.execute("SELECT DISTINCT movieid FROM `mlmovies`")
    genreNames = cur2.fetchall()

    for keyval in genreNames:
        key = keyval[0]
        #print key
        if key in dd_users_mvrating[usr[0]]:
            continue
        else:
            dd_users_mvrating[usr[0]][key] = 0



#pprint.pprint(dd_users_genre)
usr_genre_matrix = pd.DataFrame(dd_users_mvrating)
#usr_genre_matrix = usr_genre_matrix.T
#pprint.pprint(usr_genre_matrix)
usr_genre_matrix.to_csv("factorization_1_user_mvrating.csv", sep='\t')














