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
parser.add_argument("ACTOR_ID")
parser.add_argument("MODEL")
args = parser.parse_args()

#print args.ACTOR_ID

#TF MODEL
#Subtask:1 - Get tags and movieRank weight for an actor id

    #a. A dictionary to store the returned data.
data_dictionary_tf = {}
data_dictionary_tf_idf = {}

#Get sum of the rank_weights for calculating the idf value (sum of all rankweights/ sum of rank_weights for a specific tag)

cur2.execute("SELECT SUM(actor_movie_rank_weight) FROM `movie-actor`")
result0 = cur2.fetchone()
total_rank_weight = result0[0]

total_tag_newness_weight = 0

#Get total movie count for idf calculation.
cur2.execute("SELECT COUNT(movieid) FROM mlmovies")
result0 = cur2.fetchone()
total_movie_count = result0[0]


#print total_rank_weight

cur2.execute("SELECT movieid,actor_movie_rank_weight FROM `movie-actor` where actorid = %s",[args.ACTOR_ID])
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
    act_movie_id   = data1[0]
    act_movie_rank_wt = data1[1]
    actor_tag_id=""
    final_tag_wt=""


    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_weight FROM mltags WHERE movieid = %s",[act_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        #_size_count = _size_count + 1
        actor_tag_id = data2[0]
        actor_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        #tagWeight = round(((float(actor_tag_newness)/ float(total_tag_newness_weight)) * float(act_movie_rank_wt)),10)
        tagWeight = round((float(actor_tag_newness) * float(act_movie_rank_wt)), 10)
        total_tag_newness_weight = total_tag_newness_weight + tagWeight

        if tagName in data_dictionary_tf:
            data_dictionary_tf[tagName] = round((data_dictionary_tf[tagName] + tagWeight), 10)
        else:
            data_dictionary_tf[tagName] = tagWeight



#Make weight of other tags to zero and for all the tags already there, calculate the TF by dividing with total_tag
#newness_weight
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in data_dictionary_tf:
        #data_dictionary_tf[key] = round((float(data_dictionary_tf[key]) / float(total_tag_newness_weight)),10)
        continue;
    else:
        data_dictionary_tf[key] = 0

actor_model_value_tf = sorted(data_dictionary_tf.items(), key=operator.itemgetter(1), reverse=True)


#IDF CALCULATION.

if args.MODEL == "tf":
    pprint.pprint(actor_model_value_tf)
else:
    #TF-IDF CALCULATION
    cur2.execute("SELECT movieid FROM `movie-actor` where actorid = %s", [args.ACTOR_ID])
    result3 = cur2.fetchall()
    for data1 in result3:
        # print data1
        act_movie_id = data1[0]

        # Select tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s", [act_movie_id])
        result4 = cur2.fetchall()

        for data2 in result4:
            actor_tag_id = data2[0]

            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
            result2_sub = cur2.fetchall()
            tagName = result2_sub[0]

            if tagName in data_dictionary_tf_idf:
                data_dictionary_tf_idf[tagName] = int(data_dictionary_tf_idf[tagName]) + 1
            else:
                data_dictionary_tf_idf[tagName] = 1

    #Once all the tag data has been recorded, calculate the idf  and tfidf for each tag.
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

    actor_model_value_tf_idf = sorted(data_dictionary_tf_idf.items(), key=operator.itemgetter(1), reverse=True)
    pprint.pprint(actor_model_value_tf_idf)

