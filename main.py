# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import dataset
import argparse
from py2neo import authenticate, Graph,Node,Relationship,NodeSelector
from sets import Set
import datetime
import time
import pymongo
from pymongo import MongoClient
import pprint
from cassandra.cluster import Cluster,ResultSet,ResponseFuture
from cassandra import query
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ReadTimeout

# Command line argument parser
def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user',            default="neo4j",                      help='username for neo4j')
    parser.add_argument('--password',        default="333",                        help='password of the user')
    parser.add_argument('--hostname',        default="localhost:7474",             help='hostname of the Neo4j server')
    parser.add_argument('--films_path',      default="data/FILMS.txt",             help='the path of FILMS.txt file')
    parser.add_argument('--collectors_path', default="data/collectors.txt",        help='the path of collectors.txt file')
    parser.add_argument('--collect_path',    default="data/collect.txt",           help='the path of collect.txt file')
    parser.add_argument('--follow_path',     default="data/follow.txt",            help='the path of follow.txt file')
    parser.add_argument('--hostname2',       default="mongodb://localhost:27017",  help='hostname of the mongoDB server')
    parser.add_argument('--hostname3',       default="127.0.0.1",                help='hostname of the Cassandra server')

    try:
        args = parser.parse_args()
    except SystemExit as e:
        print(e)
        sys.exit()
    return args

#Read all files
def read_files(args):
    try:
        # Open files
        films_file=open(args.films_path,"r")
        collectors_file=open(args.collectors_path,"r")
        collect_file=open(args.collect_path,"r")
        follow_file=open(args.follow_path,"r")

    except IOError:
        print("The files are not found.")
        sys.exit()

    # Create and initialize local variables
    data = dataset.Data()

    # Firstly, read informations in FILMS file
    films_lines=films_file.readlines()
    for i in xrange(len(films_lines)):
        line=films_lines[i].split(' % ')
        # Create a new movie
        movie = dataset.Movie(
            ID=line[0],
            title=line[1],
            year=line[2],
            genre=line[4],
            director=line[5],
            rating=line[6],
        )
        # identify the actors of the movie
        movie.actors=Set(line[3].split(', '))
        # add the movie, actors and directors to dataset
        data.movies.add(movie)
        data.actors.update(movie.actors)
        data.directors.add(movie.director)

    # Assign id to actors and directors
    data.actors=Set([dataset.Actor(ID+2001, name) for ID, name in enumerate(data.actors)])
    data.directors=Set([dataset.Director(ID+3001, name) for ID, name in enumerate(data.directors)])

    # Read lines in collectors file
    collectors_lines=collectors_file.readlines()
    for i in xrange(len(collectors_lines)):
        line=collectors_lines[i].split('%')
        collector = dataset.Collector(
            ID=line[0],
            name=line[1],
            email=line[2]
        )
        # Add collector into collectors set in data class
        data.collectors.add(collector)

    collect_lines=collect_file.readlines()
    for i in xrange(len(collect_lines)):
        line=collect_lines[i].strip().split('%')
        data.collectings.add((line[0], line[1]))

    follow_lines=follow_file.readlines()
    for i in xrange(len(follow_lines)):
        line=follow_lines[i].strip().split('%')
        data.followings.append((line[0],line[1]))

    data.questions_list.append("1. List all actors ( userid and fullname ) who are also directors")
    data.questions_list.append("2. List all actors ( userid and fullname ) who acted in 5 or more movies.")
    data.questions_list.append("3. How many actors have acted in same movies with ’Edward Norton’?")
    data.questions_list.append("4. Which collectors collected all movies in which ’Edward Norton’ acts?")
    data.questions_list.append("5. List 10 collectors ( userid and fullname ) who collect ’The Shawshank Redemption’.")
    data.questions_list.append("6. List all userids and fullnames of xi’s which satisfy Degree(1001, xi) ≤ 3")

    return data
#################################### Neo4j ################################################
def neo4j(user,password,hostname,data):
    try:
        # Authenticate for server and connect it
        authenticate (hostname, user, password)
        graph=Graph()
    # If server is not connected :
    except Exception:
        print ("Unable to reach server.")
        sys.exit()

    graph.data("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

    #start graph operations
    start=graph.begin()

    # Create node for Movies
    for movie in data.movies:
        movie_node=Node("Movies",
            mov_id=movie.ID,
            title=movie.title,
            released_year=movie.year,
            rating=movie.rating,
            genre=movie.genre)
        start.merge(movie_node)

    # Create node for every director in data.directors
    for director in data.directors:
        director_node=Node("Directors",userid=director.ID, fullname=director.name)
        start.merge(director_node)

    # Create node for every actor in data.actors
    for actor in data.actors:
        actor_node=Node("Actors", userid=actor.ID, fullname=actor.name)
        start.merge(actor_node)

    # Create node for every collector in data.collectors
    for collector in data.collectors:
        collector_node = Node("Collectors",userid=collector.ID, fullname=collector.name, email=collector.email)
        start.merge(collector_node)

    start.commit()

    relation(data,graph)
    queries(data,graph)

# Create relationship between nodes
def relation(data,graph):
    #Related querires
    acted_in = """MATCH (m:Movies),(a:Actors)
                WHERE m.title ={title} AND a.fullname ={actor_name}
                CREATE (a)-[:ACTED_IN]->(m)"""
    directed = """MATCH (m:Movies),(d:Directors)
                WHERE m.title ={title} AND d.fullname ={director_name}
                CREATE (d)-[:DIRECTED]->(m)"""
    collects = """MATCH (c:Collectors),(m:Movies)
                WHERE c.userid ={id} AND m.mov_id ={id2}
                CREATE (c)-[:COLLECTS]->(m)"""
    follows = """MATCH (c:Collectors),(c2:Collectors)
                WHERE c.userid ={id} AND c2.userid ={id2}
                CREATE (c)-[:FOLLOWS]->(c2)"""

    for movie in data.movies:
        graph.run(directed, title=movie.title,director_name=movie.director)
        for actor in movie.actors:
            graph.run(acted_in, title=movie.title,actor_name=actor)

    for collector, movie in data.collectings:
        graph.run(collects, id=collector,id2=movie)

    for collector1,collector2 in data.followings:
        graph.run(follows, id=collector1,id2=collector2)

    return graph,data

#Write queries to a file
def queries_file(queries_list,questions_list):
    file_results = open('resultsOfNeo4j.txt', 'w')
    count=0
    for query in queries_list:
        file_results.write(questions_list[count])
        file_results.write("\n")
        if (count!=2):
            file_results.write("Userid                          Fullname \n")
        for answers in query:
            for _key, item in answers.items():
                file_results.write('{:<30}'.format(str(item)))
            file_results.write("\n")
        count+=1
        file_results.write("\n\n")

def queries(data,graph):
    # Put queries' answers into queries_list
    ########### 1. Query ###############
    data.queries_list.append(graph.run("""MATCH (a:Actors),(d:Directors)
                WITH a,d
                WHERE a.fullname = d.fullname
                RETURN DISTINCT a.userid,a.fullname ORDER BY a.userid""").data())
    ########### 2. Query ###############
    data.queries_list.append(graph.run("""MATCH (a:Actors)-[:ACTED_IN]->(m:Movies)
                WITH a,count(m) as rels
                WHERE rels >=5
                RETURN DISTINCT a.userid,a.fullname ORDER BY a.userid""").data())
    ########### 3. Query ###############
    data.queries_list.append(graph.run("""MATCH (a:Actors {fullname: "Edward Norton"})-[:ACTED_IN]->(m:Movies)
                WITH m
                MATCH (m)<-[:ACTED_IN]-(c:Actors)
                RETURN count(distinct c)-1""").data())
    ########### 4. Query ###############
    data.queries_list.append(graph.run("""MATCH (a:Actors {fullname: "Edward Norton"})-[:ACTED_IN]->(m:Movies)
                WITH m
                MATCH (m)<-[:COLLECTS]-(c:Collectors)
                WITH c
                RETURN DISTINCT c.userid,c.fullname ORDER BY c.userid""").data())
    ########### 5. Query ###############
    data.queries_list.append(graph.run("""MATCH (c:Collectors)-[:COLLECTS]->(m:Movies {title: "The Shawshank Redemption"})
                RETURN c.userid,c.fullname ORDER BY c.userid LIMIT 10""").data())
    ########### 6. Query ###############
    data.queries_list.append(graph.run("""MATCH (c1:Collectors{userid:'1001'})-[:FOLLOWS*1..3]->(c2:Collectors)
                RETURN DISTINCT c2.userid,c2.fullname ORDER BY c2.userid""").data())

    queries_file(data.queries_list,data.questions_list)

################################## MongoDB ###################################
def mongoDB(data,hostname2):
    try:
        # Connect to server
        client = MongoClient(hostname2)
    # If server is not connected :
    except Exception:
        print ("Unable to reach server.")
        sys.exit()

    # Create a database and its collections
    db = client['mydatabase']
    m_col = db ["Movies"]
    a_col1 = db ["Actors"]
    d_col1 = db ["Directors"]
    c_col1 = db ["Collectors"]
    a_col2 = db ["Acted_in"]
    d_col2 = db ["Directed"]
    c_col2 = db ["Collects"]
    c_col3 = db ["Follows"]
    #Clear collections for executing again and again.
    m_col.delete_many({})
    a_col1.delete_many({})
    d_col1.delete_many({})
    c_col1.delete_many({})
    a_col2.delete_many({})
    d_col2.delete_many({})
    c_col2.delete_many({})
    c_col3.delete_many({})

    # Insert movies to database
    for movie in data.movies:
        movie_dict = { "mov_id": movie.ID,
                       "title": movie.title,
                       "released_year": movie.year,
                       "rating": movie.rating,
                       "genre": movie.genre
                     }
        m_col.insert_one(movie_dict)

    # Insert actors with ACTED_IN relation to database
    for actor in data.actors:
        actor_dict1 = { "userid": actor.ID, "fullname": actor.name}
        a_col1.insert_one(actor_dict1)
        for movie in data.movies:
            if (actor.name in movie.actors):
                actor_dict2 = { "userid": actor.ID, "fullname": actor.name,
                                "ACTED_IN":
                                {   "mov_id": movie.ID,
                                    "title": movie.title,
                                    "released_year": movie.year,
                                    "rating": movie.rating,
                                "genre": movie.genre
                                }

                        }
                a_col2.insert_one(actor_dict2)

    # Insert directors with DIRECTED relation to database
    for director in data.directors:
        director_dict1 = { "userid": director.ID, "fullname": director.name}
        d_col1.insert_one(director_dict1)
        for movie in data.movies:
            if (director.name == movie.director):
                director_dict2 = { "userid": director.ID, "fullname": director.name,
                                   "DIRECTED":
                                   {    "mov_id": movie.ID,
                                        "title": movie.title,
                                        "released_year": movie.year,
                                        "rating": movie.rating,
                                        "genre": movie.genre
                                    }


                            }
                d_col2.insert_one(director_dict2)

    # Insert collectors with COLLECTS relation  to database
    for collector in data.collectors:
        collector_dict1 = { "userid": collector.ID, "fullname": collector.name, "email": collector.email}
        c_col1.insert_one(collector_dict1)
        for movie in data.movies:
            for item in data.collectings:
                if(item[0] == collector.ID and item[1] == movie.ID):
                    collector_dict2 = { "cuserid": collector.ID, "cfullname": collector.name, "email": collector.email,
                                        "COLLECTS":
                                        {   "mov_id": movie.ID,
                                            "title" : movie.title,
                                            "released_year": movie.year,
                                            "rating": movie.rating,
                                            "genre": movie.genre
                                        }
                                }
                    c_col2.insert_one(collector_dict2)

    # Insert collectors with FOLLOWS relation  to database
    for collector in data.collectors:
        collector_dict1 = { "userid": collector.ID, "fullname": collector.name, "email": collector.email}
        c_col1.insert_one(collector_dict1)
        for collector2 in data.collectors:
            for item2 in data.followings:
                if(item2[0] == collector.ID and item2[1] == collector2.ID):
                    collector_dict3= { "userid": collector.ID, "fullname": collector.name, "email": collector.email,
                                        "FOLLOWS":
                                        {   "userid":collector2.ID,
                                            "fullname": collector2.name,
                                            "email": collector2.email
                                        }
                                }
                    c_col3.insert_one(collector_dict3)
    queries_mongo(db)

#Writing related queries and executing them
def queries_mongo(db):
    ################ 1. QUERY ###################
    Q1 = db.Actors.aggregate( [
        {
            "$lookup":
                {
                    "from": "Directors",
                    "localField": "fullname",
                    "foreignField": "fullname",
                    "as": "actors"
                }
        },
        {
            "$unwind": "$actors"
        },
        {
            "$match": { "actors" : { "$exists": True } },
            "$match": {"fullname":{"$exists":True}},
            "$match": {"userid":{"$exists":True}}
        },
        {
            "$project":
                {
                    "fullname":1,
                    "userid": 1,
                    "_id":0
                }
        },
        {
            "$sort": { "userid" : 1}
        }
    ])
    ################## 2.QUERY #######################
    Q2 = db.Acted_in.aggregate([
        {
            "$group":
                {
                    "_id": { "fullname": '$fullname',"userid": '$userid'},
                     "count": {'$sum':1}
                }

        },

        {
            "$sort" : { "_id.userid" : 1}
        },
        {
            "$match": { "count": { "$gt": 4 }}
        },
        {
            "$project": { "count":0}
        }

    ])
    ############### 3. QUERY ########################
    Q3 = db.Acted_in.aggregate([
       {
            "$lookup":
            {
                "from": "Acted_in",
                "localField": "ACTED_IN.title",
                "foreignField": "ACTED_IN.title",
                "as": "documents"
            }
        },
        {
            "$unwind": "$documents"
        },
        {
            "$match": {"fullname":{"$eq":"Edward Norton"}}
        },
        {
            "$project":
            {
                "documents.fullname": 1,
                "_id":0
            }
        },
        {
            "$group":
            {
                "_id": "null",
                "fullnames": { "$addToSet": "$documents.fullname" }
            }
        },
        {
            "$project":
            {
                "actors": { "$setUnion": [ "$fullnames"] },
                "_id":0
            }
        },
        {
            "$project":
            {
                "numberOfActors": { "$size": "$actors" }
            }
        },
        {
            "$project":
            {
                "numberOfActors2": { "$subtract": ["$numberOfActors", 1 ] }
            }
        }
    ])
    ################# 4. QUERY #######################
    Q4 = db.Collects.aggregate([
        {
            "$lookup":
            {
                "from": "Acted_in",
                "localField": "COLLECTS.title",
                "foreignField": "ACTED_IN.title",
                "as": "documents"
            }
        },
        {
            "$unwind": "$documents"
        },
        {
            "$match": { "documents" : { "$exists": True } },
            "$match": {"cfullname":{"$exists":True}},
            "$match": {"cuserid":{"$exists":True}},
            "$match":{"COLLECTS.title":{"$exists":True}}
        },
        {
            "$project":
            {
                "cfullname": 1,
                "cuserid":1,
                "documents.ACTED_IN.title":1,
                "COLLECTS.title":1,
                "documents.fullname":1,
                "_id":0
            }
        },
        {
            "$match": {"documents.fullname":{"$eq":"Edward Norton"}}
        },
        {
            "$group":
            {
                "_id":"$cuserid",
                 "fullnames": {"$addToSet": "$cfullname"},

             }
        },
        {
            "$unwind": "$fullnames"
        },
        {
            "$sort": { "_id":1 }
        }
    ])
    ################# 5. QUERY ################################
    Q5=db.Collects.aggregate([
        {
            "$match": { "COLLECTS.title": "The Shawshank Redemption" }
        },
        {
            "$project":
            {
                "cfullname": 1,
                "cuserid":1,
                "_id":0
            }
        },
        {
            "$sort": { "cuserid":1 }
        },
        {
            "$limit":10
        }
    ])
    ################# 6. QUERY ##################
    Q6 = db.Collectors.aggregate([
        {
            "$lookup":
            {
                "from": "Follows",
                "localField": "fullname",
                "foreignField": "fullname",
                "as": "documents"
            }
        },
        {
            "$unwind": "$documents"
        },
        {
            "$match": {"documents.userid":{"$eq":"1001"}}
        },
        {
            "$lookup":
            {
                "from": "Follows",
                "localField": "documents.FOLLOWS.fullname",
                "foreignField": "fullname",
                "as": "documents2"
            }
        },
        {
            "$unwind": "$documents2"
        },
        {
            "$lookup":
            {
                "from": "Follows",
                "localField": "documents2.FOLLOWS.fullname",
                "foreignField": "fullname",
                "as": "documents3"
            }
        },
        {
            "$unwind": "$documents3"
        },
        {
            "$match": { "documents" : { "$exists": True } },
            "$match": { "documents3" : { "$exists": True } },
            "$match": {"documents2":{"$exists":True}},
            "$match":{"fullname":{"$exists":True}},
            "$match": { "userid" : { "$exists": True } },
            "$match": { "documents.FOLLOWS.userid" : { "$exists": True } },
            "$match": { "documents2.FOLLOWS.userid"  : { "$exists": True } },
            "$match": {"documents3.FOLLOWS.userid" :{"$exists":True}},
        },
        {
            "$project":
            {
                "_id":0,
                "information": ["$documents.FOLLOWS.fullname",
                "$documents2.FOLLOWS.fullname",
                "$documents3.FOLLOWS.fullname"]
            }
        },
        {
            "$unwind": "$information"
        },
        {
            "$group":{"_id": "null", "collectors": {"$addToSet": "$information"}}
        },
        {
            "$project":
            {
                "_id":0
            }
        }
    ])
    queries_MongoFile(Q1,Q2,Q3,Q4,Q5,Q6)

#Printing result into resultsOfMongoDB.txt
def queries_MongoFile(Q1,Q2,Q3,Q4,Q5,Q6):
    filee = open('resultsOfMongoDB.txt', 'w')
    filee.write("1. List all actors ( userid and fullname ) who are also directors.\n")
    filee.write("Fullname                     Userid \n")
    for answer in Q1:
        for _key,item in answer.items():
            filee.write('{:<30}'.format(str(item)))
        filee.write("\n")
    filee.write("\n\n\n")

    filee.write("2. List all actors ( userid and fullname ) who acted in 5 or more movies.\n")
    filee.write("Fullname                     Userid \n")
    for answers in Q2:
        for _key,item in answers.items():
            for _key,item2 in item.items():
                filee.write('{:<30}'.format(str(item2)))
            filee.write("\n")
    filee.write("\n\n\n")

    filee.write("3. How many actors have acted in same movies with ’Edward Norton’?\n")
    for answer in Q3:
        for _key,item in answer.items():
            filee.write('{:<30}'.format(str(item)))
        filee.write("\n")
    filee.write("\n\n\n")

    filee.write("4. Which collectors collected all movies in which ’Edward Norton’ acts?\n")
    filee.write("Userid                          Fullname \n")
    for answer in Q4:
        for _key,item in answer.items():
            filee.write('{:<30}'.format(str(item)))
        filee.write("\n")
    filee.write("\n\n\n")

    filee.write("5. List 10 collectors ( userid and fullname ) who collect ’The Shawshank Redemption’.\n")
    filee.write("Fullname                     Userid \n")
    for answer in Q5:
        for _key,item in answer.items():
            filee.write('{:<30}'.format(str(item)))
        filee.write("\n")
    filee.write("\n\n\n")

    filee.write("6. List all userids and fullnames of xi’s which satisfy Degree(1001, xi) ≤ 3\n")
    filee.write("Fullname\n")
    for answers in Q6:
        for _key,item in answers.items():
            for collector in item:
                filee.write('{:<30}'.format(str(collector)))
                filee.write("\n")

###################################### CASSANDRA ##################################################
def cassandra(data,hostname):
    try:
        # Connect to server
        cluster = Cluster([hostname])
        session = cluster.connect()
    # If server is not connected :
    except Exception:
        print ("Unable to reach server.")
        sys.exit()
    #Create a KEYSPACE and USE it
    a=session.execute("""CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }""")
    session.execute('USE mykeyspace')
    #Clear tables for executing again
    session.execute("""DROP TABLE IF EXISTS mykeyspace.movies""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.actors""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.directors""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.collectors""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.ACTED_IN""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.ACTED_IN_q3""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.DIRECTED""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.COLLECTS""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.FOLLOWS""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.FOLLOWS_q6""")
    session.execute("""DROP TABLE IF EXISTS mykeyspace.actors_directors""")

    #Create tables
    session.execute(""" CREATE TABLE IF NOT EXISTS movies(
        mov_id text,
        title text,
        released_year text,
        rating text,
        genre text,
        PRIMARY KEY(mov_id))
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS actors(
        userid int,
        fullname text,
        PRIMARY KEY(userid))
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS directors(
        userid int PRIMARY KEY,
        fullname text)
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS collectors(
        userid text PRIMARY KEY,
        fullname text,
        email text)
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS ACTED_IN(
        userid int,
        fullname text,
        movie text,
        movie_count int,
        PRIMARY KEY(fullname,movie))
    """)
    #Table creating for 3. query
    session.execute(""" CREATE TABLE IF NOT EXISTS ACTED_IN_q3(
        fullname text,
        PRIMARY KEY(fullname))
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS DIRECTED(
        userid int,
        fullname text,
        movie text,
        PRIMARY KEY(userid,movie))
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS COLLECTS(
        userid text,
        fullname text,
        movie text,
        PRIMARY KEY(userid,movie))
    """)
    session.execute(""" CREATE TABLE IF NOT EXISTS FOLLOWS(
        userid text,
        fullname text,
        userid2 text,
        fullname2 text,
        PRIMARY KEY(userid,userid2))
    """)
    #Table creating for 6. query(FOLLOWS relation levels)
    session.execute(""" CREATE TABLE IF NOT EXISTS FOLLOWS_q6(
        fullname text,
        userid int,
        email text,
        PRIMARY KEY(fullname,userid))
        WITH CLUSTERING ORDER BY (userid ASC)
    """)
    #Table creating for 1. query
    session.execute(""" CREATE TABLE IF NOT EXISTS actors_directors(
        actor_fullname text,
        actor_userid int,
        director_userid int,
        director_fullname text,
        PRIMARY KEY (actor_fullname,director_userid)
        )""")

    #fill in tables
    for movie in data.movies:
        session.execute(""" INSERT INTO movies (mov_id,title,released_year,rating,genre) VALUES (%s, %s, %s,%s, %s)""",
            (movie.ID,movie.title,movie.year,movie.rating,movie.genre)
        )
    for actor in data.actors:
        session.execute(""" INSERT INTO actors (userid,fullname) VALUES (%s, %s)""",
            (actor.ID, actor.name)
        )
    for director in data.directors:
        session.execute(""" INSERT INTO directors (userid,fullname) VALUES (%s, %s)""",
            (director.ID, director.name)
        )
    for actor in data.actors:
        count = 1
        for movie in data.movies:
            if(actor.name in movie.actors):
                session.execute(""" INSERT INTO ACTED_IN (userid,fullname,movie,movie_count) VALUES (%s, %s, %s,%s)""",
                    (actor.ID, actor.name, movie.title,count)
                )
                count+=1
    #Insert actors who acted in same movies with "Edward Norton"
    for movie in data.movies:
        if("Edward Norton" in movie.actors):
            for actor in movie.actors:
                if(actor!="Edward Norton"):
                    session.execute(""" INSERT INTO ACTED_IN_q3 (fullname) VALUES (%s)""",(actor,))
    for director in data.directors:
        for movie in data.movies:
            if(director.name == movie.director):
                session.execute(""" INSERT INTO DIRECTED (userid,fullname,movie) VALUES (%s, %s,%s)""",
                    (director.ID, director.name, movie.title)
                )
    for collector in data.collectors:
        session.execute(""" INSERT INTO collectors (userid,fullname,email) VALUES (%s, %s, %s)""",
            (collector.ID, collector.name, collector.email)
        )
        for movie in data.movies:
            for item in data.collectings:
                if(item[0] == collector.ID and item[1] == movie.ID):
                    session.execute(""" INSERT INTO COLLECTS (userid,fullname,movie) VALUES (%s, %s,%s)""",
                        (collector.ID, collector.name, movie.title)
                    )
    for collector in data.collectors:
        for collector2 in data.collectors:
            for item2 in data.followings:
                if(item2[0] == collector.ID and item2[1] == collector2.ID):
                    session.execute(""" INSERT INTO FOLLOWS (userid,fullname,userid2,fullname2) VALUES (%s, %s,%s, %s)""",
                        (collector.ID, collector.name, collector2.ID,collector2.name)
                    )
        #Insert actors who are also director
        for actor in data.actors:
            for director in data.directors:
                if(actor.name == director.name):
                    session.execute(""" INSERT INTO actors_directors (actor_fullname,actor_userid,director_userid,director_fullname) VALUES (%s,%s, %s,%s)""",
                        (actor.name,actor.ID,director.ID,director.name)
                    )
    #Insert collectors who satisfy Degree(1001, xi) ≤ 3 into mySet
    mySet = Set()
    for c in data.followings:
        #1. Level
        if(c[0]=="1001"):
            mySet.add(c[1])
            for d in data.followings:
                #2. level
                if(d[0]==c[1]):
                    mySet.add(d[1])
                    for e in data.followings:
                        #3. level
                        if(e[0]==d[1]):
                            mySet.add(e[1])
    #Then insert the collectors in the set to FOLLOWS_q6 table for 6. query
    for collector in data.collectors:
        if(collector.ID in mySet):
            session.execute(""" INSERT INTO FOLLOWS_q6 (fullname,userid,email) VALUES (%s, %s,%s)""",
                 (collector.name,int(collector.ID),collector.email))

    queries_cassandra(session,data)

#Writing related queries with CQL and executing them
def queries_cassandra(session,data):
    # Put queries' answers into queries_list_Cs
    ############## 1.QUERY ####################
    q1 = session.execute("""SELECT actor_userid AS userid,actor_fullname AS fullname FROM actors_directors""")
    data.queries_list_Cs.append(q1)

    ############# 2. QUERY #################
    q2 = session.execute("""SELECT userid,fullname FROM ACTED_IN WHERE movie_count>=5 PER PARTITION LIMIT 1 ALLOW FILTERING """)
    data.queries_list_Cs.append(q2)

    ########### 3. QUERY ##################
    q3=session.execute("""SELECT DISTINCT COUNT(fullname) FROM ACTED_IN_q3""")
    data.queries_list_Cs.append(q3)

    ############ 4. QUERY ###############
    moviess =session.execute("""SELECT movie FROM ACTED_IN WHERE fullname=%s ALLOW FILTERING""",["Edward Norton"])
    q4=session.execute("""SELECT fullname,userid FROM COLLECTS WHERE movie IN (%s,%s) PER PARTITION LIMIT 1 ALLOW FILTERING""",(moviess[0].movie,moviess[1].movie))
    data.queries_list_Cs.append(q4)

    ########## 5. QUERY ###############
    q5=session.execute("""SELECT fullname,userid FROM COLLECTS WHERE movie=%s LIMIT 10 ALLOW FILTERING """,["The Shawshank Redemption"])
    data.queries_list_Cs.append(q5)

    ########## 6. QUERY ##################
    q6=session.execute("""SELECT fullname,userid FROM FOLLOWS_q6""")
    data.queries_list_Cs.append(q6)

    #Send result of queries to queries_cassandraFile to print into resultsOfCassandra file.
    queries_cassandraFile(data.queries_list_Cs,data.questions_list)

#Print results into resultsOfCassandra.txt
def queries_cassandraFile(queries_list_Cs,questions_list):
    cassandra_file = open('resultsOfCassandra.txt', 'w')
    count=0
    for query in queries_list_Cs:
        #Printing related question to starting of results
        cassandra_file.write(questions_list[count])
        cassandra_file.write("\n")
        #This if for 3. query. Because, result of it is just a number(count of actors)
        if (count!=2):
            cassandra_file.write("Fullname                          Userid \n")
            for answer in query:
                cassandra_file.write('{:<30}'.format(str(answer.fullname)))
                cassandra_file.write('{:>10}'.format(str(answer.userid)))
                cassandra_file.write("\n")
        else:
            for answer in query:
                cassandra_file.write((str(answer)))
        count+=1
        cassandra_file.write("\n\n")

def main():
    args=arg_parser()
    data=read_files(args)
    now = datetime.datetime.now()
    neo4j(args.user,args.password,args.hostname,data)
    end = datetime.datetime.now()
    print "The time for Neo4j:", (end-now)
    now = datetime.datetime.now()
    cassandra(data,args.hostname3)
    end = datetime.datetime.now()
    print "The time for Cassandra:", (end-now)
    now = datetime.datetime.now()
    mongoDB(data,args.hostname2)
    end = datetime.datetime.now()
    print "The time for MongoDB:", (end-now)


if __name__ == '__main__':
    main()
