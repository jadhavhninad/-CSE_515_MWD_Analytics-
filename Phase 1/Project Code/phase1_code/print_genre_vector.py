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
parser.add_argument("GENRE")
parser.add_argument("MODEL")
args = parser.parse_args()

#print args.GENRE

#TF MODEL
#Subtask:1 - Get tags and Genre

    #a. A dictionary to store the returned data.
data_dictionary_tf = {}
data_dictionary_tf_idf = {}

total_tag_newness_weight = 0

#Get total movie count for idf calculation. Since we only need unique movie names, we use the mlmovies table instead of
#mlmovies_clean which has multiple entries for a movie.

cur2.execute("SELECT COUNT(movieid) FROM mlmovies")
result0 = cur2.fetchone()
total_movie_count = result0[0]


#Get all movies of a specific genre.

cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",[args.GENRE])
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
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

        if tagName in data_dictionary_tf:
            data_dictionary_tf[tagName] = round((data_dictionary_tf[tagName] + tagWeight),10)
        else:
            data_dictionary_tf[tagName] = tagWeight

# Make weight of other tags to zero.
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in data_dictionary_tf:
        data_dictionary_tf[key] = round((float(data_dictionary_tf[key]) / float(total_tag_newness_weight)),10)
    else:
        data_dictionary_tf[key] = 0

genre_model_value_tf = sorted(data_dictionary_tf.items(), key=operator.itemgetter(1), reverse=True)



#IDF CALCULATION.

if args.MODEL == "tf":
    pprint.pprint(genre_model_value_tf)
else:
    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s", [args.GENRE])
    result3 = cur2.fetchall()
    for data1 in result3:
        # print data1
        genres_movie_id = data1[0]

        # Select tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s", [genres_movie_id])
        result4 = cur2.fetchall()

        # Get the size of the returned rows. If the last row is being accessed, calculate the final log weight.
        cur2.execute("SELECT COUNT(tagid) FROM mltags WHERE movieid = %s", [genres_movie_id])
        result4_sub = cur2.fetchone()
        _tag_size = result4_sub[0]
        _size_count = 0

        for data2 in result4:
            _size_count = _size_count + 1
            genres_tag_id = data2[0]

            # Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genres_tag_id])
            result2_sub = cur2.fetchall()
            tagName = result2_sub[0]

            if tagName in data_dictionary_tf_idf:
                data_dictionary_tf_idf[tagName] = int(data_dictionary_tf_idf[tagName]) + 1
            else:
                data_dictionary_tf_idf[tagName] = 1

    #Once all data is recorded, calculate the idf and tfidf
    for key in data_dictionary_tf_idf:
        data_dictionary_tf_idf[key] = round((float(log((total_movie_count/data_dictionary_tf_idf[key]),2.71828))), 10)
        #e = 2.71828 upto 5 signigicant digits.
        data_dictionary_tf_idf[key] = round((data_dictionary_tf[key] * data_dictionary_tf_idf[key]), 10)


    #Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for key in tagName:
        if key in data_dictionary_tf_idf:
            continue;
        else:
            data_dictionary_tf_idf[key] = 0


    genre_model_value_tf_idf = sorted(data_dictionary_tf_idf.items(), key=operator.itemgetter(1), reverse=True)
    pprint.pprint(genre_model_value_tf_idf)
    #print data_dictionary

#TF-IDF