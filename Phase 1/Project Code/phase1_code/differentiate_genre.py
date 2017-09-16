from mysqlConn import DbConnect
import argparse
import operator
from math import log,fabs
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


##########################################
#General computation
#########################################

#1. Getting total number of movies in genre1 U genre2
cur2.execute("SELECT COUNT(distinct movieid) FROM mlmovies_clean where genres=%s || genres=%s",[args.GENRE1,args.GENRE2])
result0 = cur2.fetchone()
total_movie_count = result0[0]





if args.MODEL== "TF-IDF-DIFF":
    ###############################
    #MODEL = TF_IDF_DIFF
    ###############################


    #===============================================================================================
    #Subtask-1 : Calculate the weighted unique movies count returned by a tag for set of movies in genre1 U genre2
    #===============================================================================================

    cur2.execute("SELECT COUNT(distinct movieid) FROM mlmovies_clean where genres=%s || genres=%s",[args.GENRE1,args.GENRE2])
    result0 = cur2.fetchone()
    total_movie_count = result0[0]


        #Since we already have the TF value and it's data, we now generate the required data for idf.
        #IDF here will be considered as the number of movie-genre that belong to a certain tag. So the idf calculation will be
        # Total movie-genres / sum of weight of movie-genre with a particular tag

    #Calculate the total weighted count for movie-genre count for each tag.
    #weighted count for an occurance of a tag = tag_newness

    weighted_genre_movie_count={}

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres=%s || genres=%s",[args.GENRE1,args.GENRE2])
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        genre_movie_id = data1[0]
        genre_tag_id=""

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid,newness_weight FROM mltags WHERE movieid = %s",[genre_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            genre_tag_id = data2[0]
            genre_tag_newness = data2[1]

            #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]

            tagWeight = round((float(genre_tag_newness)),10)

            if tagName in weighted_genre_movie_count:
                weighted_genre_movie_count[tagName] = round((weighted_genre_movie_count[tagName] + tagWeight), 10)
            else:
                weighted_genre_movie_count[tagName] = tagWeight



    # ===============================================================================
    #Subtask-2: Get the TF , IDF and TF-IDF for the genres
    #===============================================================================

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
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]

            tagWeight = round(float(genre_tag_newness),10)
            total_tag_newness_weight = total_tag_newness_weight + tagWeight

            #For TF
            if tagName in data_dictionary_tf_genre1:
                data_dictionary_tf_genre1[tagName] = round((data_dictionary_tf_genre1[tagName] + tagWeight),10)
            else:
                data_dictionary_tf_genre1[tagName] = tagWeight



    # Make weight of other tags to zero. Calculate the tf, idf and tf-idf values for the tags that exist.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for keyVal in tagName:
        key = keyVal[0]
        if key in data_dictionary_tf_genre1:
            data_dictionary_tf_genre1[key] = round((float(data_dictionary_tf_genre1[key]) / float(total_tag_newness_weight)),10)
            data_dictionary_tf_idf_genre1[key] = round((float(log((total_movie_count/weighted_genre_movie_count[key]),2.71828))), 10)
            data_dictionary_tf_idf_genre1[key] = round((data_dictionary_tf_genre1[key] * data_dictionary_tf_idf_genre1[key]), 10)
        else:
            data_dictionary_tf_genre1[key] = 0.0

    #genre_model_value_tf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)
    #genre_model_value_tfidf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)


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
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]

            tagWeight = round(float(genre_tag_newness),10)
            total_tag_newness_weight = total_tag_newness_weight + tagWeight

            #For TF
            if tagName in data_dictionary_tf_genre2:
                data_dictionary_tf_genre2[tagName] = round((data_dictionary_tf_genre2[tagName] + tagWeight),10)
            else:
                data_dictionary_tf_genre2[tagName] = tagWeight


    # Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for keyVal in tagName:
        key=keyVal[0]
        if key in data_dictionary_tf_genre2:
            data_dictionary_tf_genre2[key] = round((float(data_dictionary_tf_genre2[key]) / float(total_tag_newness_weight)),10)
            data_dictionary_tf_idf_genre2[key] = round((float(log((total_movie_count/weighted_genre_movie_count[key]),2.71828))), 10)
            data_dictionary_tf_idf_genre2[key] = round((data_dictionary_tf_genre2[key] * data_dictionary_tf_idf_genre2[key]), 10)
        else:
            data_dictionary_tf_genre2[key] = 0.0

    #genre_model_value_tf_genre1 = sorted(data_dictionary_tf_genre1.items(), key=operator.itemgetter(1), reverse=True)
    #genre_model_value_tfidf_genre2 = sorted(data_dictionary_tf_genre2.items(), key=operator.itemgetter(1), reverse=True)

    #--------------------------------------------------------------------------------------------------------------
    #Subtask-3 : Calculate the DIFF vector
    #Manhattan distance is used since for high dimensions it works better. compared to higher order minkowski distance

    diff_vector={}

    for key in data_dictionary_tf_idf_genre1:
        if key in data_dictionary_tf_idf_genre2:
            diff_vector[key] = fabs(data_dictionary_tf_idf_genre1[key] - data_dictionary_tf_idf_genre2[key])
        else:
            diff_vector[key] = data_dictionary_tf_idf_genre1[key]

    for key in data_dictionary_tf_idf_genre2:
        if key in diff_vector:
            continue
        else:
            diff_vector[key] = data_dictionary_tf_idf_genre2[key]

    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for keyVal in tagName:
        key = keyVal[0]
        if key in diff_vector:
            continue;
        else:
            diff_vector[key] = 0.0



    genre_diff = sorted(diff_vector.items(), key=operator.itemgetter(1), reverse=True)

    #pprint.pprint(genre_model_value_tfidf_genre1)
    #pprint.pprint(genre_model_value_tfidf_genre2)
    pprint.pprint(genre_diff)


'''
elif args.MODEL == "P-DIFF1" :

    ###############################
    #MODEL = P-DIFF-1
    ###############################

    # ===============================================================================
    #Subtask-1: Calculate the number of movies for a given tag for genre1 and genre2
    #and total movies in genre1
    #================================================================================

    dd_r1_genre1 = {}
    dd_m1_genre2 = {}
    M = total_movie_count #Movies in genre1 U genre2

    cur2.execute("SELECT count(movieid) FROM `mlmovies_clean` where genres = %s",[args.GENRE1])
    result1 = cur2.fetchall()
    R = result1[0] #Movies in genre1


    #Calculation for genre1.

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",[args.GENRE1])
    result1 = cur2.fetchall()
    for data1 in result1:
        genre_movie_id = data1[0]

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s",[genre_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            genre_tag_id = data2[0]

            #Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]

            #For TF
            if tagName in dd_r1_genre1:
                dd_r1_genre1[tagName] = (dd_r1_genre1[tagName] + 1)
            else:
                dd_r1_genre1[tagName] = 1


    #Calculation for genre2

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",[args.GENRE2])
    result1 = cur2.fetchall()
    for data1 in result1:
        genre_movie_id = data1[0]

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s",[genre_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            genre_tag_id = data2[0]

            #Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]

            #For TF
            if tagName in dd_m1_genre2:
                dd_m1_genre2[tagName] = (dd_m1_genre2[tagName] + 1)
            else:
                dd_m1_genre2[tagName] = 1


    #Subtask:2 - Calculate the pdiff1 using the given formula
    pdiff_wt_genre1={}

    for tag in dd_r1_genre1:
        r = dd_r1_genre1[tag]
        m=0
        if tag in dd_m1_genre2:
            m=dd_m1_genre2[tag]

        val1,val2,val3,val4

        #r = 0 means that the tag never occurs for a genre.
        #R=r means that the tag occurs for every movie of the genre, so its frequency is 1 and
        #discriminating power is 0 . In both the scenarios, we ignore such a tag.
        
        if r==0 or r==R:
            val1 = 0
        else:
            val1 = float(r / R-r)

        if r==m ||
            val2 = float((m-r)/(M-m-R+r))





        val3 = float(r / R)
        





        val4 = float(m-r / M-R)

'''


















