from mysqlConn import DbConnect
import csv

db = DbConnect()
db_conn = db.get_connection()

cur2 = db_conn.cursor();

#import data to mysql db from the csv datafiles
"""
csv_data = csv.reader(file('../phase1_dataset/imdb-actor-info.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `imdb-actor-info`(actorid, name, gender) VALUES(%s, %s, %s)',row)

db_conn.commit()


csv_data = csv.reader(file('../phase1_dataset/mlmovies.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `mlmovies`(movieid, moviename, genres) VALUES(%s, %s, %s)',row)

db_conn.commit()


csv_data = csv.reader(file('../phase1_dataset/mlratings.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `mlratings`(movieid, userid, imdbid, rating, timestamp) VALUES(%s, %s, %s, %s, %s)',row)

db_conn.commit()

csv_data = csv.reader(file('../phase1_dataset/mltags.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `mltags`(userid, movieid, tagid, timestamp) VALUES(%s, %s, %s, %s)',row)

db_conn.commit()


csv_data = csv.reader(file('../phase1_dataset/mlusers.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `mlusers`(userid) VALUES(%s)',row)

db_conn.commit()

cur2.execute('Create table `movie-actor`(movieid varchar(10) NOT NULL, actorid varchar(10) NOT NULL, actor_movie_rank int(10) NOT NULL)')
db_conn.commit()

csv_data = csv.reader(file('../../phase1_dataset/movie-actor.csv'))
for row in csv_data:
    cur2.execute('INSERT INTO `movie-actor`(movieid, actorid, actor_movie_rank) VALUES(%s, %s, %s)',row)
    db_conn.commit()

db_conn.close()

print "All data has been Imported"


"""