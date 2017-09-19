from mysqlConn import DbConnect
from datetime import datetime
from math import exp,log

db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();


#------------------------------------------------------
#TASK 1 : General Pre-processing
#------------------------------------------------------
#Using FEATURE SCALING.

#Using just normalization on the timestamp values.
fmt = '%Y-%m-%d %H:%M:%S'
cur2.execute("Select timestamp from `mltags` order by timestamp asc limit 1")
result = cur2.fetchone()
ts_min = datetime.strptime(result[0], fmt)

cur2.execute("Select timestamp from `mltags` order by timestamp desc limit 1")
result = cur2.fetchone()
ts_max = datetime.strptime(result[0], fmt)

ts_diff = ts_max - ts_min

cur2.execute("Alter table `mltags` add column newness_wt_norm_nolog varchar(20) NOT NULL")

cur2.execute("Select timestamp from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'
row_count = 0

for timedata in result:
    ts_val = datetime.strptime(timedata[0], fmt)
    ts_wt = (ts_val - ts_min).total_seconds() / ts_diff.total_seconds()
    cur2.execute("UPDATE `mltags` set newness_wt_norm_nolog = %s where timestamp = %s", (ts_wt, timedata[0]))
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "General preprocessing done."


print "Starting preprocessing for actor tag vector..."
#----------------------------------------------
#TASK-2: Pre-processing for actor tag vector
#----------------------------------------------

#Calculating the actor-movie rank weights based on normalized values instead of the exponential decay.

cur2.execute("select actor_movie_rank from `movie-actor` order by actor_movie_rank asc limit 1")
result = cur2.fetchone()
rank_max = float(result[0])

cur2.execute("select actor_movie_rank from `movie-actor` order by actor_movie_rank desc limit 1")
result = cur2.fetchone()
rank_min = float(result[0])

rank_diff = rank_max - rank_min

cur2.execute("Alter table `movie-actor` add column rank_wt_norm varchar(20) NOT NULL")

cur2.execute("Select actor_movie_rank from `movie-actor`")
result = cur2.fetchall()

row_count=0
for rankdata in result:
    ts_wt = float((float(rankdata[0]) - rank_min) / rank_diff)
    cur2.execute("UPDATE `movie-actor` set rank_wt_norm = %s where actor_movie_rank = %s", (ts_wt, rankdata[0]))
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()


#Since we already have the TF value and it's data, we now generate the required data for idf.
#IDF here will be considered as the number of movie-actors that belong to a certain tag. So the idf calculation will be
# Total movie-actors / sum of weight of movie-actors with a particular tag


#Calculate the total weighted count of movie-actor for each tag.
#weighted count for an occurance of a tag = tag_newness * actor-rank-weight

#Create a column weighted_movie-actor count for storing the values
cur2.execute("Alter table `genome-tags` add column total_wt_movie_actor_count varchar(20) NULL")
db_conn.commit()

weighted_actor_movie_count={}

cur2.execute("SELECT movieid,rank_wt_norm FROM `movie-actor`")
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
        actor_tag_id = data2[0]
        actor_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [actor_tag_id])
        result2_sub = cur2.fetchone()
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
row_count = 0

for keyval in tagName:
    key = keyval[0]
    if key in weighted_actor_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_actor_count= %s where tag=%s",(weighted_actor_movie_count[key],key))
        if row_count >= 1000:
            db_conn.commit()
            row_count = 0

db_conn.commit();
print "done"


print "Starting preprocessing for genre tag vectors..."
#==========================================================
#TASK - 3 : PRE - PROCESSING FOR GENRE VECTOR
#==========================================================

#SUB -TASK 1 - Cleaning the mlmovies table. Getting single row for a single genre.

    #a. Create a new table mlmovies_clean that has a single entry for a single genre.
    #b. For each entry in the mlmovies create an entry in mlmovies_clean that has a unique genre entry.

cur2.execute("create table `mlmovies_clean`(movieid varchar(10) NOT NULL, moviename varchar(200) NOT NULL, genres varchar(200) NOT NULL)")

query1 = "SELECT * FROM `mlmovies`"
cur2.execute(query1)
result1 = cur2.fetchall()
row_count = 0

#For each tagID get the movie list
for entry in result1:
    mvid = entry[0]
    mvname = entry[1]
    combo_genres = entry[2].split("|")

    #Add new row for each genre.
    for genre in combo_genres:
        cur2.execute('INSERT INTO `mlmovies_clean`(movieid, moviename, genres) VALUES(%s, %s, %s)', (mvid,mvname,genre))

    if row_count >= 1000:
        db_conn.commit()
        row_count = 0

db_conn.commit()


#Sub-TASK 2 : creating a weighted_genre_movie_count for calculating the idf value.

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of movie-genre that belong to a certain tag. So the idf calculation will be
    # Total movie-genres / sum of weight of movie-genres with a particular tag

#Calculate the total weighted count for movie-genre count for each tag.
#weighted count for an occurance of a tag = tag_newness

#Create a column weighted_movie-genre count for storing the values
cur2.execute("Alter table `genome-tags` add column total_wt_movie_genre_count varchar(20) NULL")
#db_conn.commit()

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
        result2_sub = cur2.fetchone()
        tagName = result2_sub[0]

        tagWeight = round((float(genre_tag_newness)),10)

        if tagName in weighted_genre_movie_count:
            weighted_genre_movie_count[tagName] = round((weighted_genre_movie_count[tagName] + tagWeight), 10)
        else:
            weighted_genre_movie_count[tagName] = tagWeight


#Update the total_weighted_genre_count for all the tags already there, in the same column in genome-tags

cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()
row_count = 0

for keyval in tagName:
    key = keyval[0]
    if key in weighted_genre_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_genre_count= %s where tag=%s",(weighted_genre_movie_count[key],key))
        if row_count >= 1000:
            db_conn.commit()
            row_count = 0

db_conn.commit();

print "done"

print "Starting pre-processing for user vector..."
#==========================================================
#TASK 4 : PRE - PROCESSING FOR USER VECTOR
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
    user_movie_id = data1[0]
    user_tag_id=""


    #Select distint tagIDs for the movieID
    cur2.execute("SELECT tagid,newness_wt_norm_nolog FROM mltags WHERE movieid = %s",[user_movie_id])
    result2 = cur2.fetchall()

    for data2 in result2:
        user_tag_id = data2[0]
        user_tag_newness = data2[1]

        #Get the tag_name for the tagID. For each tag weight, add the rank_weight as well.
        cur2.execute("SELECT tag FROM `genome-tags` WHERE tagID = %s", [user_tag_id])
        result2_sub = cur2.fetchone()
        tagName = result2_sub[0]

        tagWeight = round((float(user_tag_newness)),10)

        if tagName in weighted_user_movie_count:
            weighted_user_movie_count[tagName] = round((weighted_user_movie_count[tagName] + tagWeight), 10)
        else:
            weighted_user_movie_count[tagName] = tagWeight


#Update the total_weighted_genre_count for all the tags already there, in the same column in genome-tags

cur2.execute("SELECT tag FROM `genome-tags`")
tagName = cur2.fetchall()

for keyval in tagName:
    key = keyval[0]
    if key in weighted_user_movie_count:
        cur2.execute("UPDATE `genome-tags` set total_wt_movie_user_count= %s where tag=%s",(weighted_user_movie_count[key],key))
        if row_count >= 1000:
            db_conn.commit()
            row_count = 0

db_conn.commit();

print "done"
print "all Preprocessing has been completed"






