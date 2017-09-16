from mysqlConn import DbConnect
from datetime import datetime
from math import exp,log

db = DbConnect()
db_conn = db.get_connection()

cur2 = db_conn.cursor();

#------------------------------------------------------
#TASK 1 : General Pre-processing
#------------------------------------------------------
'''
#Subtask  - 1 : Get a list of tagIDs from the genome-tag table - not used so far

query1 = "Select tagid from `genome-tags`"
cur2.execute(query1)
result = cur2.fetchall()

for id in result:
    #print id[0]
    cur2.execute("SELECT COUNT(*) from `mltags` where tagid = %s", (id))
    idCount = cur2.fetchone()
    #print idCount[0]
    cur2.execute("UPDATE `genome-tags` set tag_count = %s where tagid= %s ",(idCount,id))
    db_conn.commit()

'''
'''
#Subtask - 2 :  Calculate the time weights of tags timestamp in mltags table -
#1.using exponential decay

#add a column newness_weight in the table mltags.

cur2.execute("Select timestamp from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'

    # k = decay constant. The decay constant is used so that the exponential
    #  values stay within  limit (after trying with 1,0.1,0.01,0.001) and the final weight is rounded to 5 significant digits
    # beacause that helps to maintain a good level of precision

k=0.001

for timedata in result:
    ts1   = datetime.strptime('2009-01-04 18:58:49', fmt)
    ts2   = datetime.strptime(timedata[0], fmt)
    tdiff = ts1 - ts2
    #The diff is in days
    tdiff = int(round(tdiff.total_seconds() / (3600*24)))
    #print(tdiff)
    ts_wt = round(exp(-k*tdiff),5)
    #print (ts_wt)
    cur2.execute("UPDATE `mltags` set newness_weight = %s where timestamp = %s",(ts_wt,timedata[0]))
    db_conn.commit()

'''

'''
print("----------------------------")
#2. Using Inverse-squared decay

#This function is not selected as using similar decay function, the exponential decay gives better
values than inverse squared decay function.

cur2.execute("Select timestamp from `mltags` limit 10")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'

    # k = decay constant. The decay constant is used so that the exponential
    #  values stay within  limit (after trying with 1,0.1,0.01,0.001) and the final weight is rounded to 5 significant digits
    # beacause that helps to maintain good precision

k=0.001

for timedata in result:
    ts1   = datetime.strptime('2009-01-04 18:58:49', fmt)
    ts2   = datetime.strptime(timedata[0], fmt)
    tdiff = ts1 - ts2
    #The diff is in days
    tdiff = int(tdiff.total_seconds() / (3600*24))
    #print(tdiff)
    ts_wt = round(1/pow(k*tdiff,2),5)
    #print(ts_wt)
    cur2.execute("UPDATE `mltags` set newness_weight = %s where timestamp = %s",(ts_wt,timedata[0]))
    db_conn.commit()

'''

#Using FEATURE SCALING.
'''
#NO USE OF TAKING LOG, DIRECTLY NORMALIZE
#Data is positively skewed.Most of the data values lie in date 2006 (50%)Using log to reduce skewness

cur2.execute("Alter table `mltags` add column ts_log varchar(20) NOT NULL")

cur2.execute("Select timestamp from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'

for timedata in result:
    ts = datetime.strptime(timedata[0], fmt)
    ts_val = ts - datetime.strptime("1970-01-1 00:00:00",fmt)
    ts_val = ts_val.total_seconds()
    cur2.execute("UPDATE `mltags` set ts_log = %s where timestamp = %s", (log(ts_val,2), timedata[0]))
    db_conn.commit()
'''
'''
#Using normalization on the log timestamp values (feature scaling)

cur2.execute("Select ts_log from `mltags` order by ts_log asc limit 1")
result = cur2.fetchone()
ts_min = float(result[0])

cur2.execute("Select ts_log from `mltags` order by ts_log desc limit 1")
result = cur2.fetchone()
ts_max = float(result[0])

ts_diff = ts_max - ts_min

#cur2.execute("Alter table `mltags` add column newness_wt_norm varchar(20) NOT NULL")

cur2.execute("Select ts_log from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'

for timedata in result:
    ts_wt = float((float(timedata[0]) - ts_min) / ts_diff)
    cur2.execute("UPDATE `mltags` set newness_wt_norm = %s where ts_log = %s", (ts_wt, timedata[0]))
    db_conn.commit()
'''
'''
#Using just normalization on the timestamp values.
fmt = '%Y-%m-%d %H:%M:%S'
cur2.execute("Select timestamp from `mltags` order by timestamp asc limit 1")
result = cur2.fetchone()
ts_min = datetime.strptime(result[0], fmt)

cur2.execute("Select timestamp from `mltags` order by timestamp desc limit 1")
result = cur2.fetchone()
ts_max = datetime.strptime(result[0], fmt)

ts_diff = ts_max - ts_min

#cur2.execute("Alter table `mltags` add column newness_wt_norm_nolog varchar(20) NOT NULL")

cur2.execute("Select timestamp from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'

for timedata in result:
    ts_val = datetime.strptime(timedata[0], fmt)
    ts_wt = (ts_val - ts_min).total_seconds() / ts_diff.total_seconds()
    cur2.execute("UPDATE `mltags` set newness_wt_norm_nolog = %s where timestamp = %s", (ts_wt, timedata[0]))
    db_conn.commit()

'''

#----------------------------------------------
#TASK-2: Pre-processing for actor tag vector
#----------------------------------------------

'''
#Subtask-3 : Calculate the weights for movie-actor rank in movie actor table.

    #Similar exponential decay function is implemented for the actor rank weights, but in reverse order.
    #The least rank will have heigher weight and the max rank will have lower weight.

cur2.execute("select actor_movie_rank from `movie-actor`")
#cur2.execute("select * from `movie-actor` where actor_movie_rank_weight=''")
result = cur2.fetchall()
    # k = decay constant. The decay constant is used so that the exponential
    #  values stay within  limit (after trying with 1,0.1,0.01,0.001) and the final weight is rounded to 5 significant digits
    # beacause that helps to maintain a good level of precision

k=0.01

for rankdata in result:
    rdiff = 1 - int(rankdata[0])
    #print(rdiff)
    rank_wt = round(exp(k*rdiff),5)
    #print (rank_wt)
    cur2.execute("UPDATE `movie-actor` set actor_movie_rank_weight = %s where actor_movie_rank = %s",(rank_wt,rankdata[0]))
    db_conn.commit()

'''

'''
--------------------------------------------------------------------------------------------------
#cur2.execute("select actor_movie_rank from `movie-actor`")
# b : Updating the weight of very large values to 0 - need further analysis.
cur2.execute("select * from `movie-actor` where actor_movie_rank_weight=''")
result = cur2.fetchall()
    # k = decay constant. The decay constant is used so that the exponential
    #  values stay within  limit (after trying with 1,0.1,0.01,0.001) and the final weight is rounded to 5 significant digits
    # beacause that helps to maintain a good level of precision

#k=0.01

for rankdata in result:
    #rdiff = 1 - int(rankdata[0])
    rank_wt = '0'
    #print(rdiff)
    #rank_wt = round(exp(k*rdiff),5)
    #print (rank_wt)
    cur2.execute("UPDATE `movie-actor` set actor_movie_rank_weight = %s where actor_movie_rank = %s",(rank_wt,rankdata[0]))
    db_conn.commit()
-------------------------------------------------------------------------------------------------
'''

'''

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of movie-actors that belong to a certain tag. So the idf calculation will be
    # Total movie-actors / sum of weight of movie-actors with a particular tag
    #But we will use : rank_weight sum / sum of rank weights for a particular tag.
    #Both raw idf and tw_idf will be calculated for comparison.

#Calculate the total weighted count for movie-actor count for each tag.
#weighted count for an occurance of a tag = tag_newness * actor-rank-weight

#Create a column weighted_movie-actor count for storing the values
cur2.execute("Alter table `genome-tags` add column total_wt_movie_actor_count varchar(20) NULL")
db_conn.commit()

weighted_actor_movie_count={}

cur2.execute("SELECT movieid,actor_movie_rank_weight FROM `movie-actor`")
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
        actor_tag_id = data2[0]
        actor_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        #tagWeight = round(((float(actor_tag_newness)/ float(total_tag_newness_weight)) * float(act_movie_rank_wt)),10)
        tagWeight = round((float(actor_tag_newness) * float(act_movie_rank_wt)), 10)

        if tagName in weighted_actor_movie_count:
            weighted_actor_movie_count[tagName] = round((weighted_actor_movie_count[tagName] + tagWeight), 10)
        else:
            weighted_actor_movie_count[tagName] = tagWeight



#Update the total_weighted_actor_count for all the tags already there, in the same column in genome-tags

cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in weighted_actor_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_actor_count= %s where tag=%s",(weighted_actor_movie_count[key],key))
        db_conn.commit();
    else:
        weighted_actor_movie_count[key] = 0
'''

'''
#Subtask-1 - this is not required.
    #Calulate the raw frequency of the actors with specific tags and update in genome-tags.raw_frequency table.

query1 = "SELECT tagID FROM `genome-tags`"
cur2.execute(query1)
result1 = cur2.fetchall()

#For each tagID get the movie list
for tg_id in result1:
    cur2.execute("SELECT movieid FROM `mltags` WHERE tagid = %s",[tg_id[0]])
    result2 = cur2.fetchall()
    raw_act=0

    #For each movie, count the actors in that movie
    for mv_id in result2:
        cur2.execute("SELECT count(actorid) FROM `movie-actor` WHERE movieid=%s",[mv_id[0]])
        act_count = cur2.fetchone()
        raw_act = raw_act + int(act_count[0])

    #Update the total number of actor count for the tagID
    cur2.execute("UPDATE `genome-tags` set raw_actors= %s where tagID= %s ", (raw_act,tg_id))
    db_conn.commit()
'''

'''
#Subtask-2
    #Calulate the sum of rank_wt of the actors with specific tags and update in genome-tags.wt_rank_sum table.

query1 = "SELECT tagID FROM `genome-tags`"
cur2.execute(query1)
result1 = cur2.fetchall()

#For each tagID get the movie list
for tg_id in result1:
    cur2.execute("SELECT movieid FROM `mltags` WHERE tagid = %s",[tg_id[0]])
    result2 = cur2.fetchall()
    sum_rank_wt=0

    #For each movie, count the actors in that movie
    for mv_id in result2:
        cur2.execute("SELECT sum(actor_movie_rank_weight) FROM `movie-actor` WHERE movieid=%s",[mv_id[0]])
        rank_wt = cur2.fetchone()
        sum_rank_wt = round(sum_rank_wt + float(rank_wt[0]),5)

    #Update the total number of actor count for the tagID
    cur2.execute("UPDATE `genome-tags` set rank_wt_sum= %s where tagID= %s ", (sum_rank_wt,tg_id))
    db_conn.commit()

'''

#==========================================================
#TASK - 3 : PRE - PROCESSING FOR GENRE VECTOR
#==========================================================

'''
#SUB -TASK 1 - Cleaning the mlmovies table. Getting single row for a single genre.

    #a. Create a new table mlmovies_clean that has a single entry for a single genre.
    #b. For each entry in the mlmovies create an entry in mlmovies_clean that has a unique genre entry.

query1 = "create table `mlmovies_clean`(movieid varchar(10) NOT NULL, moviename varchar(200) NOT NULL, genres varchar(200 NOT NULL))"
db_conn.commit()

query1 = "SELECT * FROM `mlmovies`"
cur2.execute(query1)
result1 = cur2.fetchall()

#For each tagID get the movie list
for entry in result1:
    mvid = entry[0]
    mvname = entry[1]
    combo_genres = entry[2].split("|")

    #Add new row for each genre.
    for genre in combo_genres:
        #print(genre)
        cur2.execute('INSERT INTO `mlmovies_clean`(movieid, moviename, genres) VALUES(%s, %s, %s)', (mvid,mvname,genre))

    db_conn.commit()

'''

#Sub-TASK 2 : creating a weighted_genre_movie_count for calculating the idf value.

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of movie-genre that belong to a certain tag. So the idf calculation will be
    # Total movie-genres / sum of weight of movie-genres with a particular tag

#Calculate the total weighted count for movie-genre count for each tag.
#weighted count for an occurance of a tag = tag_newness

#Create a column weighted_movie-genre count for storing the values
cur2.execute("Alter table `genome-tags` add column total_wt_movie_genre_count varchar(20) NULL")
db_conn.commit()

weighted_genre_movie_count={}

cur2.execute("SELECT movieid FROM `mlmovies_clean`")
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
    genre_movie_id   = data1[0]
    genre_tag_id=""
    final_tag_wt=""


    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_wt_norm_nolog FROM mltags WHERE movieid = %s",[genre_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        genre_tag_id = data2[0]
        genre_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        tagWeight = round((float(genre_tag_newness)),10)

        if tagName in weighted_genre_movie_count:
            weighted_genre_movie_count[tagName] = round((weighted_genre_movie_count[tagName] + tagWeight), 10)
        else:
            weighted_genre_movie_count[tagName] = tagWeight


#Update the total_weighted_genre_count for all the tags already there, in the same column in genome-tags

cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in weighted_genre_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_genre_count= %s where tag=%s",(weighted_genre_movie_count[key],key))
        db_conn.commit();
    else:
        weighted_genre_movie_count[key] = 0



#==========================================================
#TASK - 3 : PRE - PROCESSING FOR USER VECTOR
#==========================================================

#Sub-TASK 2 : creating a weighted_user_movie_count for calculating the idf value.

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of user-movie that belong to a certain tag. So the idf calculation will be
    # Total movie-user / sum of weight of movie-user for a particular tag

#Calculate the total weighted count for movie-user count for each tag.
#weighted count for an occurance of a tag = tag_newness

#Create a column weighted_movie-genre count for storing the values
cur2.execute("Alter table `genome-tags` add column total_wt_movie_user_count varchar(20) NULL")
db_conn.commit()

weighted_user_movie_count={}

cur2.execute("SELECT movieid FROM `mlratings`")
result1 = cur2.fetchall()
for data1 in result1:
    #print data1
    user_movie_id   = data1[0]
    user_tag_id=""
    final_tag_wt=""


    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_wt_norm_nolog FROM mltags WHERE movieid = %s",[user_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        user_tag_id = data2[0]
        user_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [user_tag_id])
        result2_sub = cur2.fetchall()
        tagName = result2_sub[0]

        tagWeight = round((float(user_tag_newness)),10)

        if tagName in weighted_user_movie_count:
            weighted_user_movie_count[tagName] = round((weighted_user_movie_count[tagName] + tagWeight), 10)
        else:
            weighted_user_movie_count[tagName] = tagWeight


#Update the total_weighted_genre_count for all the tags already there, in the same column in genome-tags

cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for key in tagName:
    if key in weighted_user_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_user_count= %s where tag=%s",(weighted_user_movie_count[key],key))
        db_conn.commit();
    else:
        weighted_user_movie_count[key] = 0


'''
This is not required since we are now using movie-actor count
#Get the count of number of unique actors returned by a tag.
#Pre-calculate : For each actor, for each of his movie, increment the tag. If a tag is already incremented, skip it.
#Add a table that stores tag id and unique actor it returns.

data_dictionary_alltagcount={}

cur2.execute("SELECT actorid from `imdb-actor-info`")
result0 = cur2.fetchall()
for eachActor in result0:

    data_dictionary_actortagcount={}

    cur2.execute("SELECT movieid FROM `movie-actor` where actorid = %s",[eachActor[0]])
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        act_movie_id   = data1[0]
        actor_tag_id=""
        final_tag_wt=""

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s",[act_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            #_size_count = _size_count + 1
            actor_tag_id = data2[0]

            #Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
            result2_sub = cur2.fetchall()
            tagName = result2_sub[0]

            if tagName in data_dictionary_actortagcount:
                continue
            else:
                data_dictionary_actortagcount[tagName] = 1

    #Once we have a unique values for all tags that refer to an actor, add it to the already calculated tagcount
    #for other actors.
    for key in data_dictionary_actortagcount:
        if key in data_dictionary_alltagcount:
            data_dictionary_alltagcount[key] =  data_dictionary_alltagcount[key] + data_dictionary_actortagcount[key]
        else:
            data_dictionary_alltagcount[key] = data_dictionary_actortagcount[key]

#once we have count of unique actors returned by a tag, update it in the table tag-actorCount.


for key in data_dictionary_alltagcount:
    cur2.execute('INSERT INTO `tag_actorCount`(tag, actorCount) VALUES(%s, %s)', (key, data_dictionary_alltagcount[key]))

db_conn.commit()

'''






