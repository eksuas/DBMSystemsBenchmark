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

# Create and initialize local variables
data = dataset.Data()

# Command line argument parser
def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user',            default="neo4j",                help='username for neo4j')
    parser.add_argument('--password',        default="333",                  help='password of the user')
    parser.add_argument('--hostname',        default="localhost:7474",       help='hostname of the server')
    parser.add_argument('--films_path',      default="data/FILMS.txt",       help='the path of FILMS.txt file')
    parser.add_argument('--collectors_path', default="data/collectors.txt",  help='the path of collectors.txt file')
    parser.add_argument('--collect_path',    default="data/collect.txt",     help='the path of collect.txt file')
    parser.add_argument('--follow_path',     default="data/follow.txt",      help='the path of follow.txt file')
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
        data.collectings.append((line[0], line[1]))

    follow_lines=follow_file.readlines()
    for i in xrange(len(follow_lines)):
        line=follow_lines[i].strip().split('%')
        data.followings.append((line[0],line[1]))
    return data

def mongoDB(data):
    try:
        # Connect to server
        client = MongoClient('mongodb://localhost:27017')
    # If server is not connected :
    except Exception:
        print ("Unable to reach server.")
        sys.exit()

    # Create a database and its collections
    db = client['mydatabase']
    m_col = db ["Movies"]
    a_col = db ["Actors"]
    d_col = db ["Directors"]
    c_col = db ["Collectors"]

    m_col.delete_many({})
    a_col.delete_many({})
    d_col.delete_many({})
    c_col.delete_many({})

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
        for movie in data.movies:
            if (actor.name in movie.actors):
                actor_dict = { "userid": actor.ID, "fullname": actor.name,
                                "ACTED_IN": [
                                {   "mov_id": movie.ID,
                                    "title": movie.title,
                                    "released_year": movie.year,
                                    "rating": movie.rating,
                                    "genre": movie.genre
                                }
                            ]
                        }
                a_col.insert_one(actor_dict)

    # Insert directors with DIRECTED relation to database
    for director in data.directors:
        for movie in data.movies:
            if (director.name == movie.director):
                director_dict = { "userid": director.ID, "fullname": director.name,
                                  "DIRECTED": [
                                {   "mov_id": movie.ID,
                                    "title": movie.title,
                                    "released_year": movie.year,
                                    "rating": movie.rating,
                                    "genre": movie.genre
                                }
                            ]

                        }
                d_col.insert_one(director_dict)

    # Insert collectors with COLLECTS relation  to database
    for collector in data.collectors:
        for movie in data.movies:
            for item in data.collectings:
                if(item[0] == collector.ID and item[1] == movie.ID):
                    collector_dict1 = { "userid": collector.ID, "fullname": collector.name, "email": collector.email,
                                        "COLLECTS": [
                                        {   "mov_id": movie.ID,
                                            "title" : movie.title,
                                            "released_year": movie.year,
                                            "rating": movie.rating,
                                            "genre": movie.genre
                                        }
                                    ]
                                }
                    c_col.insert_one(collector_dict1)

    # Insert collectors with FOLLOWS relation  to database
    for collector in data.collectors:
        for collector2 in data.collectors:
            for item2 in data.followings:
                if(item2[0] == collector.ID and item2[1] == collector2.ID):
                    collector_dict2= { "userid": collector.ID, "fullname": collector.name, "email": collector.email,
                                        "FOLLOWS": [
                                        {   "userid":collector2.ID,
                                            "fullname": collector2.name,
                                            "email": collector2.email
                                        }
                                    ]
                                }
                    c_col.insert_one(collector_dict2)


def main():
    args=arg_parser()
    data=read_files(args)
    #Learn time difference for neo4j operations
    now = datetime.datetime.now()
    mongoDB(data)
    end = datetime.datetime.now()
    print (end-now)


if __name__ == '__main__':
    main()
