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

cur2.execute("SELECT SUM(rank_wt_norm) FROM `movie-actor`")
result0 = cur2.fetchone()
total_rank_weight = result0[0]

total_tag_newness_weight = 0

#Get total movie-acto count for idf calculation. Here every movie-actor row value is a document , ie a combination
#for which a particular tag occurs.
cur2.execute("SELECT COUNT(distinct movieid,actorid) FROM `movie-actor`")
result0 = cur2.fetchone()
total_documents = float(result0[0])


#print total_rank_weight

cur2.execute("SELECT movieid,rank_wt_norm FROM `movie-actor` where actorid = %s",[args.ACTOR_ID])
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
    act_movie_id   = data1[0]
    act_movie_rank_wt = data1[1]
    actor_tag_id=""
    final_tag_wt=""


    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_wt_norm_nolog FROM mltags WHERE movieid = %s",[act_movie_id])
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



#Make weight of other tags to zero and for all the tags already there, calculate the TF by dividing with total_tag_newness_weight
cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in data_dictionary_tf:
        #print 'curval',key
        data_dictionary_tf[key] = round((float(data_dictionary_tf[key]) / float(total_tag_newness_weight)),10)
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

        # Select tagIDs for the movieID. we choose distinct since the total_weighted_movie_actor_count is already precomputed.
        cur2.execute("SELECT distinct(tagid) FROM mltags WHERE movieid = %s", [act_movie_id])
        result4 = cur2.fetchall()

        for data2 in result4:
            actor_tag_id = data2[0]

            cur2.execute("SELECT tag,total_wt_movie_actor_count FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]
            tag_movie_actor_count = result2_sub[1]

            if tagName in data_dictionary_tf_idf:
                continue
            else:
                data_dictionary_tf_idf[tagName] = float(tag_movie_actor_count)


    #Once all the tag data has been recorded, calculate the idf  and tfidf for each tag.
    #Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tgName = cur2.fetchall()

    for key in tgName:
        keyval =  key[0]
        if keyval in data_dictionary_tf_idf:
            data_dictionary_tf_idf[keyval] = round((float(log((total_documents / data_dictionary_tf_idf[keyval]), 2.71828))),10)
            data_dictionary_tf_idf[keyval] = round(float(float(data_dictionary_tf[key]) * float(data_dictionary_tf_idf[keyval])), 10)
        else:
            data_dictionary_tf_idf[keyval] = 0


    actor_model_value_tf_idf = sorted(data_dictionary_tf_idf.items(), key=operator.itemgetter(1), reverse=True)
    pprint.pprint(actor_model_value_tf_idf)

