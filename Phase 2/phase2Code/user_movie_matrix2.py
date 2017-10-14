'''
Here we get a  the user_movie matrix by using dot product of the  transpose of user_genre matrix with movie_genre matrix
for a specific user.
'''


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
parser = argparse.ArgumentParser()
parser.add_argument("USER")
args = parser.parse_args()

'''
#------------------------------------------------------
#TASK 1 : General Pre-processing
#------------------------------------------------------
#Using FEATURE SCALING for the tag newnewss.

#Using just normalization on the timestamp values and limiting between 0.5 and 1.
fmt = '%Y-%m-%d %H:%M:%S'
cur2.execute("Select timestamp from `mltags` order by timestamp asc limit 1")
result = cur2.fetchone()
ts_min = datetime.strptime(result[0], fmt)

cur2.execute("Select timestamp from `mltags` order by timestamp desc limit 1")
result = cur2.fetchone()
ts_max = datetime.strptime(result[0], fmt)

ts_diff = ts_max - ts_min

cur2.execute("Alter table `mltags` add column newness_wt varchar(20) NOT NULL")

cur2.execute("Select timestamp from `mltags`")
result = cur2.fetchall()
fmt = '%Y-%m-%d %H:%M:%S'
row_count = 0

for timedata in result:
    ts_val = datetime.strptime(timedata[0], fmt)
    ts_wt = (ts_val - ts_min).total_seconds() / ts_diff.total_seconds()
    balanced_wt = float(0.5 + 0.5*ts_wt)
    cur2.execute("UPDATE `mltags` set newness_wt = %s where timestamp = %s", (balanced_wt, timedata[0]))
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "General preprocessing done."


#==========================================================
#TASK - 1 : PRE - PROCESSING
#==========================================================

#SUB -TASK 1 - Cleaning the mlmovies table. Getting single row for a single genre.

    #a. Create a new table mlmovies_clean that has a single entry for a single genre.
    #b. For each entry in the mlmovies create an entry in mlmovies_clean that has a unique genre entry.

cur2.execute("create table `mlmovies_clean`(movieid varchar(10) NOT NULL, moviename varchar(200) NOT NULL, year varchar(4) NOT NULL, genres varchar(200) NOT NULL)")

query1 = "SELECT * FROM `mlmovies`"
cur2.execute(query1)
result1 = cur2.fetchall()
row_count = 0

#For each tagID get the movie list
for entry in result1:
    mvid = entry[0]
    mvname = entry[1]
    year = entry[2]
    combo_genres = entry[3].split("|")

    #Add new row for each genre.
    for genre in combo_genres:
        cur2.execute('INSERT INTO `mlmovies_clean`(movieid, moviename, year, genres) VALUES(%s, %s, %s, %s)', (mvid,mvname,year,genre))

    if row_count >= 1000:
        db_conn.commit()
        row_count = 0

db_conn.commit()
#----------------------------------------------------------------------

#====================================================================
#TASK - 2 : Weights of movies based on year using exponential decay
#====================================================================

#Get the max year.
cur2.execute("SELECT max(year) FROM mlmovies")
max_year = int(cur2.fetchone()[0])

#add a column year_weight in the table mlmovies.
cur2.execute("Alter table `mlmovies` add column year_wt FLOAT(15) NOT NULL")

cur2.execute("SELECT year FROM `mlmovies`")
result = cur2.fetchall()

    # k = decay constant. Appropriate decay constant is used so that the exponential
    #  values stay within  limit (after trying with 1,0.1,0.01,0.001) and

k=0.1

for movie_year in result:
    current_year   = int(movie_year[0])
    diff = max_year - current_year
    movie_wt = float(exp(-k*diff))
    cur2.execute("UPDATE `mlmovies` set year_wt = %s where year = %s",(movie_wt,movie_year[0]))
    db_conn.commit()

#-------------------------------------------------------------------


print "Starting preprocessing for genre tag vectors..."
#==========================================================
#TASK - 3 : PRE - PROCESSING FOR GENRE VECTOR
#==========================================================

#Sub-TASK 2 : creating a weighted_genre_movie_count for calculating the idf value.

    #Since we already have the TF value and it's data, we now generate the required data for idf.
    #IDF here will be considered as the number of movie-genre that belong to a certain tag. So the idf calculation will be
    # Total movie-genres / sum of weight of movie-genres with a particular tag

#Calculate the total weighted count for movie-genre count for each tag.
#weighted count for an occurance of a tag = tag_newness

#Create a column weighted_movie-genre count for storing the values
####cur2.execute("Alter table `genome-tags` add column total_wt_movie_genre_count varchar(20) NULL")
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
    cur2.execute("SELECT tagid,newness_wt FROM mltags WHERE movieid = %s",[genre_movie_id])
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
'''
#===================================================================
#Task-4: Generate the tag vectors for each genre using tf-idf model
#===================================================================

genre_vectors = {}

cur2.execute("SELECT distinct genres FROM mlmovies_clean")
resultgen = cur2.fetchall()
for genre in resultgen:
    genre_vectors[genre[0]] = {}
    #TF MODEL
    #Subtask:1 - Get tags and Genre

        #a. A dictionary to store the returned data.
    data_dictionary_tf = {}
    data_dictionary_tf_idf = {}

    total_tag_newness_weight = 0

    #Get total movie-genre count for idf calculation.
    #For each tag, a document here is row in mlmovies_clean table that has genre-movie.

    cur2.execute("SELECT COUNT(distinct movieid,genres) FROM mlmovies_clean")
    result0 = cur2.fetchone()
    total_documents = result0[0]


    #Get all movies of a specific genre.

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s",genre)
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        genre_movie_id = data1[0]

        #Select distint tagIDs for the movieID
        cur2.execute("SELECT tagid,newness_wt FROM mltags WHERE movieid = %s",[genre_movie_id])
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

            if tagName in data_dictionary_tf:
                data_dictionary_tf[tagName] = round((data_dictionary_tf[tagName] + tagWeight),10)
            else:
                data_dictionary_tf[tagName] = tagWeight

    # Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for keyval in tagName:
        key = keyval[0]
        if key in data_dictionary_tf:
            data_dictionary_tf[key] = round((float(data_dictionary_tf[key]) / float(total_tag_newness_weight)),10)
        else:
            data_dictionary_tf[key] = 0

    #genre_model_value_tf = sorted(data_dictionary_tf.items(), key=operator.itemgetter(1), reverse=True)

    #IDF CALCULATION.

    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s", genre)
    result3 = cur2.fetchall()
    for data1 in result3:
        genres_movie_id = data1[0]

        # Select tagIDs for the movieID
        cur2.execute("SELECT tagid FROM mltags WHERE movieid = %s", [genres_movie_id])
        result4 = cur2.fetchall()

        for data2 in result4:
            genres_tag_id = data2[0]

            # Get the tag_name for the tagID.
            cur2.execute("SELECT tag,total_wt_movie_genre_count FROM `genome-tags` WHERE tagID = %s", [genres_tag_id])
            result2_sub = cur2.fetchone()
            tagName = result2_sub[0]
            tag_movie_genre_count = result2_sub[1]

            if tagName in data_dictionary_tf_idf:
                continue;
            else:
                data_dictionary_tf_idf[tagName] = float(tag_movie_genre_count)

    #Once all data is recorded, calculate the idf and tfidf
    #Make weight of other tags to zero.
    cur2.execute("SELECT tag FROM `genome-tags`")
    tagName = cur2.fetchall()

    for key in tagName:
        keyval = key[0]
        if keyval in data_dictionary_tf_idf:
            data_dictionary_tf_idf[keyval] = round((float(log((total_documents / data_dictionary_tf_idf[keyval]), 2.71828))), 10)
            data_dictionary_tf_idf[keyval] = round((data_dictionary_tf[keyval] * data_dictionary_tf_idf[keyval]), 10)
        else:
            data_dictionary_tf_idf[keyval] = 0


    genre_model_value_tf_idf = sorted(data_dictionary_tf_idf.items(), key=operator.itemgetter(0), reverse=True)
    #pprint.pprint(genre_model_value_tf_idf)

    #Update in the master list which will then be converted to dataframe and written to a file.
    for key in genre_model_value_tf_idf:
        genre_vectors[genre[0]][key[0]] = key[1]


#pprint.pprint(dd_users_movie)
genre_tag_matrix = pd.DataFrame(genre_vectors)
#genre_tag_matrix = genre_tag_matrix.T
#pprint.pprint(genre_tag_matrix)
genre_tag_matrix.to_csv("genre-tag_vectors.csv", sep='\t')


#========================================================================================
# Task 5 - Generate movie-genere vector matrix.
#========================================================================================

#For a genre assigned to movie, the movie-genre value is sum of tags that belong to a movie
#and seen for that genre

movie_genre_vectors={}

#Get all movies
cur2.execute("SELECT movieid FROM `mlmovies`")
result1 = cur2.fetchall()
for mv_name in result1:
    #print data1
    movie_genre_vectors[mv_name[0]]={}
    user_movie_id = {mv_name[0],}

    cur2.execute("SELECT genres FROM `mlmovies_clean` where movieid = %s", user_movie_id)
    result2 = cur2.fetchall()
    for gen_list in result2:

        #Select all tags for the movie
        cur2.execute("SELECT tagid FROM `mltags` where movieid = %s", user_movie_id)
        result3 = cur2.fetchall()
        #print "result3 = ",result3
        for vals in result3:
            #print vals
            cur2.execute("SELECT tag FROM `genome-tags` where tagid = %s", vals)
            tag_name = cur2.fetchone()[0]

            #For a genre of a movie add the tag weights for the genre.The sum is the value of
            #movie-genre vector.
            if gen_list[0] in movie_genre_vectors[mv_name[0]]:
                movie_genre_vectors[mv_name[0]][gen_list[0]] += genre_vectors[gen_list[0]][tag_name]
            else:
                #Add mean for the first time.
                movie_genre_vectors[mv_name[0]][gen_list[0]] = genre_vectors[gen_list[0]][tag_name] + genre_tag_matrix[gen_list[0]].mean()

        #Add mean of a genre from genre-tag vector matrix
        # This is done to handle the case: If a movie has no tags, but still belongs to a genre,
        if result3 == ():
            #print "mean = ", genre_tag_matrix[gen_list[0]].mean()
            movie_genre_vectors[mv_name[0]][gen_list[0]] = genre_tag_matrix[gen_list[0]].mean()


    #Make other genre values 0
    cur2.execute("SELECT distinct genres FROM `mlmovies_clean`")
    result_gen = cur2.fetchall()
    for gen in result_gen:
        if gen[0] in movie_genre_vectors[mv_name[0]]:
            #print movie_genre_vectors[mv_name[0]][gen[0]]
            continue
        else:
            movie_genre_vectors[mv_name[0]][gen[0]] = 0


#pprint.pprint(dd_users_movie)
movie_genre_matrix = pd.DataFrame(movie_genre_vectors)
#movie_genre_matrix = movie_genre_matrix.T
#pprint.pprint(genre_tag_matrix)
movie_genre_matrix.to_csv("movie_genre_matrix.csv", sep='\t')

#========================================================
#Task:6 - Calculate the user-genre Matrix
#========================================================

dd_users_genre = {}


#Get all the users. Lmiting the user to 10 for now.
cur2.execute("SELECT userid FROM `mlusers` limit 10")
result0 = cur2.fetchall();
for usr in result0:
    #print usr[0]
    dd_users_genre[usr[0]] = {}

    #Get all movies watched(and hence rated) by each user.
    cur2.execute("SELECT movieid, rating FROM `mlratings` where userid = %s",usr)
    result1 = cur2.fetchall()
    for data1 in result1:
        #print data1
        user_movie_id = {data1[0],}
        user_movie_rating = data1[1]

        #for each movie add the genre weight.IN this way, genres will be ranked based on highest watched to
        #lowest watched for a user. Movies tagged or rated newly will have higher rank as year_wt is more.
        #But if movie rating is bad, then rank will drop as we multiply both year and rating

        cur2.execute("SELECT genres FROM `mlmovies_clean` where movieid = %s", user_movie_id)
        result2 = cur2.fetchall()
        for vals in result2:
            #print vals
            cur2.execute("SELECT year_wt FROM `mlmovies` where genres = %s", vals)
            mv_weight = cur2.fetchone()[0]

            if vals[0] in dd_users_genre[usr[0]]:
                dd_users_genre[usr[0]][vals[0]] += (mv_weight * int(user_movie_rating))
            else:
                dd_users_genre[usr[0]][vals[0]] = mv_weight * int(user_movie_rating)

    #WE need to do this again for mltags because it does not have a rating, so we give an avg below
    # rating of 2.

    # Get all movies tagged by each user. If movie is only tagged and not rated, then give rating of 2 (avg).
    cur2.execute("SELECT movieid FROM `mltags` where userid = %s", usr)
    result2 = cur2.fetchall()
    for data in result2:
        #print data1
        user_movie_id = {data[0],}

        cur2.execute("SELECT genres FROM `mlmovies_clean` where movieid = %s", user_movie_id)
        result2 = cur2.fetchall()
        for vals in result2:

            cur2.execute("SELECT year_wt FROM `mlmovies` where genres = %s", vals)
            mv_weight = cur2.fetchone()[0]

            if vals[0] in dd_users_genre[usr[0]]:
                dd_users_genre[usr[0]][vals[0]] += (mv_weight * 2)
            else:
                dd_users_genre[usr[0]][vals[0]] = mv_weight * 2


    #Make rating of other genres to 0.
    cur2.execute("SELECT DISTINCT genres FROM `mlmovies_clean`")
    genreNames = cur2.fetchall()

    for keyval in genreNames:
        key = keyval[0]
        #print key
        if key in dd_users_genre[usr[0]]:
            continue
        else:
            dd_users_genre[usr[0]][key] = 0



#pprint.pprint(dd_users_genre)
usr_genre_matrix = pd.DataFrame(dd_users_genre)
#usr_genre_matrix = usr_genre_matrix.T
#pprint.pprint(usr_genre_matrix)
usr_genre_matrix.to_csv("user_genre_matrix.csv", sep='\t')


#=====================================================================
#Task:7 - Generate a rating of all movies seen by a user and sort them
#=====================================================================


#Get list of user watched movies
#This data can also be precomputed and stored
userWatchedMovies = []
cur2.execute("SELECT movieid FROM `mlratings` where userid = %s",[args.USER])
result0 = cur2.fetchall()
for data in result0:
    userWatchedMovies.append(data[0])

cur2.execute("SELECT movieid FROM `mltags` where userid = %s",[args.USER])
result0 = cur2.fetchall()
for data in result0:
    userWatchedMovies.append(data[0])

print "-----Watched movies-------"
for watched_ids in userWatchedMovies:
    cur2.execute("SELECT moviename,genres FROM `mlmovies` where movieid = %s", {watched_ids, })
    print cur2.fetchone()



#For a user do (user_genre_matrix)T * (movie_genre_matrix)
#Return top 5 unwatched movies in the generated vector

#The user_genre matrix is already a transpose, so we can multiply it directly.
#Multiply a row vector of movie_genre with column vector of user_genre.

user_movie_vec = {}
user_genre_vec = pd.DataFrame(usr_genre_matrix[args.USER])

#print "----user_genre vector Transpose---"
#print user_genre_vec

for column in movie_genre_matrix:
    #Skip watched movies.
    if column in userWatchedMovies:
        continue
    else:
        #print "------movie-genre-vector--------"
        movie_genre_vec = pd.DataFrame(movie_genre_matrix[column])
        #print movie_genre_vec.T
        temp_df = pd.DataFrame(movie_genre_vec.T.dot(user_genre_vec))
        user_movie_vec[column] = temp_df.iat[0,0]

#print user_movie_vec
Unwatched_user_movie_ratings = sorted(user_movie_vec,key=operator.itemgetter(1), reverse=True)
#print Unwatched_user_movie_ratings

#Print top 5 movies with max ratings
print "-------------------------------------"
print "-------Top 5 Recommended movies------"
for i in range(0,5,1):
    cur2.execute("SELECT moviename,genres FROM `mlmovies` where movieid = %s",[Unwatched_user_movie_ratings[i]])
    print cur2.fetchone()


'''

count=0
recommend=[]

print "-------------------------------"
print "-------Recommended movies------"

for keys in sorted_x:
    #print keys[0]
    cur2.execute("SELECT movieid FROM `mlmovies_clean` where genres = %s", {keys[0],})
    result0 = cur2.fetchall()
    for data in result0:
        if data[0] in userWatchedMovies:
            continue
        else:
            recommend.append(data[0])
            count+=1
            if count==5 : break
    if count == 5: break

for rec_ids in recommend:
    #print rec_ids
    cur2.execute("SELECT moviename,genres FROM `mlmovies` where movieid = %s", {rec_ids, })
    print cur2.fetchone()
'''
'''
similarity_user = usr_movie_matrix.dot(usr_movie_matrix.T) + 1e-9
norms = np.array([np.sqrt(np.diagonal(similarity_user))])
similarity_user = ( similarity_user / (norms * norms.T))

print norms

similarity_movie = usr_movie_matrix.T.dot(usr_movie_matrix) + 1e-9
norms = np.array([np.sqrt(np.diagonal(similarity_movie))])
similarity_movie = ( similarity_movie / (norms * norms.T) )



#prediction = similarity_user.dot(usr_movie_matrix) / np.array([np.abs(similarity_user).sum(axis=1)]).T



prediction = prediction[test_matrix.nonzero()].flatten()
test_vector = test_matrix[test_matrix.nonzero()].flatten()
mse = mean_squared_error(prediction, test_vector)

print 'MSE = ' + str(mse)
'''
