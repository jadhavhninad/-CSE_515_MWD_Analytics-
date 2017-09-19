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
parser.add_argument("USER")
parser.add_argument("MODEL")
args = parser.parse_args()

#print args.USER

#TF MODEL
#Subtask:1 - Get tags

    #a. A dictionary to store the returned data.
data_dictionary_tf = {}
data_dictionary_tf_idf = {}

total_tag_newness_weight = 0

#Get total movie count for idf calculation. Since we only need unique movie names, we use the mlmovies table instead of
#mlmovies_clean which has multiple entries for a movie.

cur2.execute("SELECT Count(Distinct movieid,userid) FROM mlratings")
result0 = cur2.fetchone()
total_document = result0[0]


#Get all movies of a watched(and hence rated) by a user.

cur2.execute("SELECT movieid FROM `mlratings` where userid = %s",[args.USER])
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
    user_movie_id = data1[0]

    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_wt_norm_nolog FROM mltags WHERE movieid = %s",[user_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        user_tag_id = data2[0]
        user_tag_newness = data2[1]

        #Get the tag_name for the tagID.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [user_tag_id])
        result2_sub = cur2.fetchone()
        tagName = result2_sub[0]

        tagWeight = round(float(user_tag_newness),10)
        total_tag_newness_weight = total_tag_newness_weight + tagWeight

        if tagName in data_dictionary_tf:
            data_dictionary_tf[tagName] = round((data_dictionary_tf[tagName] + tagWeight),10)
        else:
            data_dictionary_tf[tagName] = tagWeight

#Make weight of other tags to zero.
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for keyval in tagName:
    key = keyval[0]
    if key in data_dictionary_tf:
        data_dictionary_tf[key] = round((float(data_dictionary_tf[key]) / float(total_tag_newness_weight)), 10)
    else:
        data_dictionary_tf[key] = 0


user_model_value_tf = sorted(data_dictionary_tf.items(), key=operator.itemgetter(1), reverse=True)


#IDF CALCULATION.

if args.MODEL == "tf":
    pprint.pprint(user_model_value_tf)
else:
    cur2.execute("SELECT movieid FROM `mlratings` where userid = %s", [args.USER])
    result3 = cur2.fetchall()
    for data1 in result3:
        # print data1
        user_movie_id = data1[0]

        # Select tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s", [user_movie_id])
        result4 = cur2.fetchall()

        for data2 in result4:
            user_tag_id = data2[0]

            # Get the tag_name for the tagID.
            cur2.execute("SELECT tag,total_wt_movie_user_count FROM `genome-tags` WHERE tagID = %s", [user_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]
            user_tag_genre_count = result2_sub[1]

            if tagName in data_dictionary_tf_idf:
                continue;
            else:
                data_dictionary_tf_idf[tagName] = float(user_tag_genre_count)

    #Calculate the idf and tfidf for existing tags.
    # Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for key in tagName:
        keyval = key[0]
        if keyval in data_dictionary_tf_idf:
            data_dictionary_tf_idf[keyval] = round((float(log((total_document / data_dictionary_tf_idf[keyval]), 2.71828))),10)
            data_dictionary_tf_idf[keyval] = round((data_dictionary_tf[keyval] * data_dictionary_tf_idf[keyval]), 10)
        else:
            data_dictionary_tf_idf[keyval] = 0


    user_model_value_tf_idf = sorted(data_dictionary_tf_idf.items(), key=operator.itemgetter(1), reverse=True)
    pprint.pprint(user_model_value_tf_idf)
    #print data_dictionary


#TF-IDF