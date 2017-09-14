from mysqlConn import DbConnect
import argparse
import operator
from math import log
import pprint


#DB connector and curosor
db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

#Argument parser
parser = argparse.ArgumentParser()
parser.add_argument("GENRE1")
parser.add_argument("GENRE2")
parser.add_argument("MODEL")
args = parser.parse_args()

#--------------------
#MODEL = TF_IDF_DIFF
#--------------------

#Subtask:1 - Get the unique movies returned for a tag among movies in genre1 U genre2
#--------------------------------------------------------------------------------------

#So for each of the movies in that set, get the count of movies returned by a tag.

tag_count_unique_movies={}

#Get total movie count (genre1 U genre2) for idf calculation.

cur2.execute("SELECT COUNT(distinct movieid) FROM mlmovies_clean where genres=%s || genres=%s",[args.GENRE1,args.GENRE2])
result0 = cur2.fetchone()
total_movie_count = result0[0]

#Get the distinct movies in genre1 U genre2
cur2.execute("SELECT distinct movieid FROM mlmovies_clean where genres=%s || genres=%s",[args.GENRE1,args.GENRE2])
result1 = cur2.fetchall()
for movieval in result1:

    # Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s", [movieval[0]])
    result2 = cur2.fetchall()

    for data2 in result2:
        genre_tag_id = data2[0]

        # Get the tag_name for the tagID.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        if tagName in tag_count_unique_movies:
            tag_count_unique_movies[tagName] = tag_count_unique_movies[tagName] + 1
        else:
            tag_count_unique_movies[tagName] = 1



#Subtask-2: Get the TF , IDF and TF-IDF for the genres
#------------------------------------------------------

data_dictionary_tf_genre1 = {}
data_dictionary_tf_idf_genre1 = {}
total_tag_newness_weight = 0

#Get all movies of genre 1.

cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",[args.GENRE1])
result1 = cur2.fetchall()
for data1 in result1:
    genre_movie_id = data1[0]

    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_weight FROM mltags WHERE movieid = %s",[genre_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        genre_tag_id = data2[0]
        genre_tag_newness = data2[1]

        #Get the tag_name for the tagID.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        tagWeight = round(float(genre_tag_newness),10)
        total_tag_newness_weight = total_tag_newness_weight + tagWeight

        #For TF
        if tagName in data_dictionary_tf_genre1:
            data_dictionary_tf_genre1[tagName] = round((data_dictionary_tf_genre1[tagName] + tagWeight),10)
        else:
            data_dictionary_tf_genre1[tagName] = tagWeight

        #For IDF
        if tagName in data_dictionary_tf_idf_genre1:
            data_dictionary_tf_idf_genre1[tagName] = data_dictionary_tf_idf_genre1[tagName] + 1
        else:
            data_dictionary_tf_idf_genre1[tagName] = 1


# Make weight of other tags to zero. Calculate the tf, idf and tf-idf values for the tags that exist.
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in data_dictionary_tf_genre1:
        data_dictionary_tf_genre1[key] = round((float(data_dictionary_tf_genre1[key]) / float(total_tag_newness_weight)),10)
        data_dictionary_tf_idf_genre1[key] = round((float(log((total_movie_count/data_dictionary_tf_idf_genre1[key]),2.71828))), 10)
        data_dictionary_tf_idf_genre1[key] = round((data_dictionary_tf_genre1[key] * data_dictionary_tf_idf_genre1[key]), 10)
    else:
        data_dictionary_tf_genre1[key] = 0

#genre_model_value_tf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)
genre_model_value_tfidf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)


#Get all movies of a specific genre 2.
#--------------------------------------
data_dictionary_tf_genre2 = {}
data_dictionary_tf_idf_genre2 = {}
total_tag_newness_weight = 0

cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",[args.GENRE2])
result1 = cur2.fetchall()
for data1 in result1:
    genre_movie_id = data1[0]

    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_weight FROM mltags WHERE movieid = %s",[genre_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        genre_tag_id = data2[0]
        genre_tag_newness = data2[1]

        #Get the tag_name for the tagID.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        tagWeight = round(float(genre_tag_newness),10)
        total_tag_newness_weight = total_tag_newness_weight + tagWeight

        #For TF
        if tagName in data_dictionary_tf_genre2:
            data_dictionary_tf_genre2[tagName] = round((data_dictionary_tf_genre2[tagName] + tagWeight),10)
        else:
            data_dictionary_tf_genre2[tagName] = tagWeight


        #For IDF
        if tagName in data_dictionary_tf_idf_genre1:
            data_dictionary_tf_idf_genre1[tagName] = data_dictionary_tf_idf_genre1[tagName] + 1
        else:
            data_dictionary_tf_idf_genre1[tagName] = 1


# Make weight of other tags to zero.
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in data_dictionary_tf_genre2:
        data_dictionary_tf_genre2[key] = round((float(data_dictionary_tf_genre2[key]) / float(total_tag_newness_weight)),10)
        data_dictionary_tf_idf_genre2[key] = round((float(log((total_movie_count/data_dictionary_tf_idf_genre2[key]),2.71828))), 10)
        data_dictionary_tf_idf_genre2[key] = round((data_dictionary_tf_genre2[key] * data_dictionary_tf_idf_genre2[key]), 10)
    else:
        data_dictionary_tf_genre2[key] = 0

#genre_model_value_tf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)
genre_model_value_tfidf_genre2 = sorted(data_dictionary_tf_genre2.items(), key=operator.itemgetter(1), reverse=True)

#--------------------------------------------------------------------------------------------------------------
#Subtask-3 : Calculate the DIFF vector
#Manhattan distance is used since for high dimensions it works better. compared to higher order minkowsi distance

genre_diff_vector={}

