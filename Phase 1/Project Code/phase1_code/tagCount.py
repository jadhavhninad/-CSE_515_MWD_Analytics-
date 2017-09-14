from mysqlConn import DbConnect
from datetime import datetime
from math import exp

db = DbConnect()
db_conn = db.get_connection()

cur2 = db_conn.cursor();


#TASK 1 : Calculating the data for TF model

'''
#Subtask  - 1 : Get a list of tagIDs from the genome-tag table

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

#This function is not selected as using similar decay function, the exponential decay gives better normalized
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
#Subtask-4 : Calculating weighted tf for each genome ie tagid.
    #This is calculated as sum(newness_weight for all tag occurence)/tagCount

query1 = "Select tagid from `genome-tags`"
cur2.execute(query1)
result = cur2.fetchall()

for id in result:
    #Get all the newness_weight entries for a tagid.
    cur2.execute("SELECT newness_weight  from `mltags` where tagid = %s", (id))
    tag_values = cur2.fetchall()

    #calculate the total tag_newness
    tag_newness_sum=0
    for tag_entry in tag_values:
        tag_newness_sum = tag_newness_sum + float(tag_entry[0])

    #Get idCount
    cur2.execute("SELECT tag_count from `genome-tags` where tagid = %s", (id))
    idCount = cur2.fetchone()

    #Calculate the average time_weighted tag frequency
    avg_tw_tf = round(tag_newness_sum / int(idCount[0]),5)

    #print avg_tw_tf

    #Update in the genome-tag table as avg_timeweighted_tf
    cur2.execute("UPDATE `genome-tags` set avg_tw_tf = %s where tagid= %s ",(avg_tw_tf,id))
    db_conn.commit()

    #Update total_timeweighted_tf
    cur2.execute("UPDATE `genome-tags` set total_timeweighted_tf = %s where tagid= %s ",(tag_newness_sum,id))
    db_conn.commit()

'''
'''
#Subtask-5 : Calculating the classic raw_tf and timeweighted_tf for the tagid.

    #a. raw_tf = tag_count / total entries in mltags

query1 = "Select tag_count from `genome-tags`"
cur2.execute(query1)
result = cur2.fetchall()

for countId in result:

    sub_query1 = "Select count(*) from mltags"
    cur2.execute(sub_query1)
    tag_total = cur2.fetchone();
    raw_tf = round(float(countId[0]) / float(tag_total[0]),5)

    cur2.execute("UPDATE `genome-tags` set classic_raw_tf = %s where tag_count= %s ",(raw_tf,countId))
    db_conn.commit()
'''

'''
    #b. classic_tw_tf = total_tw_tf / sum of all newness_weights in mltags

query1 = "SELECT total_timeweighted_tf FROM `genome-tags`"
cur2.execute(query1)
result = cur2.fetchall()

for wtId in result:

    sub_query1 = "SELECT sum(newness_weight) FROM mltags"
    cur2.execute(sub_query1)
    newness_total = cur2.fetchone();
    cls_tw_tf = round(float(wtId[0]) / float(newness_total[0]),5)

    cur2.execute("UPDATE `genome-tags` set classic_tw_tf = %s where total_timeweighted_tf= %s ", (cls_tw_tf, wtId))
    db_conn.commit()

'''

#----------------------------------------------
#TASK-2: Calculating the data for TF-IDF model.
#----------------------------------------------

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of actors that belong to a certain tag. So the idf calculation will be
    # Total actors / no. of actors with a particular tag
    #But we will use : rank_weight sum / sum of rank weights for a particular tag.
    #Both raw idf and tw_idf will be calculated for comparison.

'''
#Subtask-1
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

'''
#TASK 3 - Cleaning the mlmovies table. Getting single row for a single genre.

    #a. Create a new table mlmovies_clean that has a single entry for a single genre.
    #b. For each entry in the mlmovies create an entry in mlmovies_clean that has a unique genre entry.

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
'''
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

'''
#Get the count of number of unique genres returned by a tag.
#Pre-calculate : For each genre, for each of its movie, increment the tag. If a tag is already incremented, skip it.
#Add a table that stores tag id and unique actor it returns.

data_dictionary_alltagcount={}

cur2.execute("SELECT DISTINCT genres from `mlmovies_clean`")
result0 = cur2.fetchall()
for eachGenre in result0:

    data_dictionary_genretagcount={}

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres=%s",[eachGenre[0]])
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        genre_movie_id   = data1[0]

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s",[genre_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            #_size_count = _size_count + 1
            genre_tag_id = data2[0]

            #Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [genre_tag_id])
            result2_sub = cur2.fetchall()
            tagName = result2_sub[0]

            if tagName in data_dictionary_genretagcount:
                continue
            else:
                data_dictionary_genretagcount[tagName] = 1

    #Once we have a unique values for all tags that refer to an actor, add it to the already calculated tagcount
    #for other actors.
    for key in data_dictionary_genretagcount:
        if key in data_dictionary_alltagcount:
            data_dictionary_alltagcount[key] =  data_dictionary_alltagcount[key] + data_dictionary_genretagcount[key]
        else:
            data_dictionary_alltagcount[key] = data_dictionary_genretagcount[key]

#once we have count of unique actors returned by a tag, update it in the table tag-actorCount.


for key in data_dictionary_alltagcount:
    cur2.execute('UPDATE `tag_actorCount`set genreCount = %s where tag=%s', (data_dictionary_alltagcount[key],key))

db_conn.commit()

'''

'''
#First insert a list of all unique users who have rated a movie, in a table.
cur2.execute("SELECT DISTINCT userid from `mlratings`")
result0 = cur2.fetchall()
for eachUser in result0:
    cur2.execute('INSERT INTO `rating_users_distinct`(userid) VALUES(%s)',[eachUser[0]])

db_conn.commit()
'''

#Get the count of number of unique users returned by a tag.
#Pre-calculate : For each user, for each of movie they watched, increment the tag. If a tag is already incremented, skip it.
#Add a table that stores tag id and unique actor it returns.

data_dictionary_alltagcount={}

cur2.execute("SELECT userid from `rating_users_distinct`")
result0 = cur2.fetchall()
for eachUser in result0:

    data_dictionary_usertagcount={}

    cur2.execute("SELECT movieid FROM `mlratings` where userid=%s",[eachUser[0]])
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        user_movie_id   = data1[0]

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s",[user_movie_id])
        result2 = cur2.fetchall()

        for data2 in result2:
            #_size_count = _size_count + 1
            user_tag_id = data2[0]

            #Get the tag_name for the tagID.
            cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [user_tag_id])
            result2_sub = cur2.fetchall()
            tagName = result2_sub[0]

            if tagName in data_dictionary_usertagcount:
                continue
            else:
                data_dictionary_usertagcount[tagName] = 1

    #Once we have a unique values for all tags that refer to an actor, add it to the already calculated tagcount
    #for other actors. In case the dictionary is still empty, don't do anything and move to next user.
    if bool(data_dictionary_usertagcount) !=False:
        for key in data_dictionary_usertagcount:
            if key in data_dictionary_alltagcount:
                data_dictionary_alltagcount[key] =  data_dictionary_alltagcount[key] + data_dictionary_usertagcount[key]
            else:
                data_dictionary_alltagcount[key] = data_dictionary_usertagcount[key]

#once we have count of unique actors returned by a tag, update it in the table tag-actorCount.
print "Updating the DB now."

for key in data_dictionary_alltagcount:
    #cur2.execute('SELECT `userCount` FROM `tag_actorCount` where tag=%s', (key))
    #val = cur2.fetchone()
    #new_val = int(val[0]) + data_dictionary_alltagcount[key]
    #print key
    #print new_val
    cur2.execute('UPDATE `tag_actorCount`set userCount = %s where tag=%s', (data_dictionary_alltagcount[key],key))
    db_conn.commit()





