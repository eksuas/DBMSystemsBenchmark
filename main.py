# -*- coding: utf-8 -*-
import dataset
import argparse
from py2neo import authenticate, Graph,Node,Relationship,NodeSelector
import sys
from sets import Set

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
    data.actors=Set([dataset.Actor(ID+2000, name) for ID, name in enumerate(data.actors)])
    data.directors=Set([dataset.Director(ID+3000, name) for ID, name in enumerate(data.directors)])

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
    follow_lines=follow_file.readlines()
    return data,collect_lines,follow_lines

#Neo4j operations
def neo4j(user,password,hostname,data):

    try:
        # Authenticate for server and connect it
        authenticate (hostname, user, password)
        graph=Graph()
    # If server is not connected :
    except Exception as e:
        print ("Unable to reach server.")
        sys.exit()
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
        #Create it
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
    return graph

# Create relationship between nodes
def relation(data,graph,collect_lines,follow_lines):

    for movie in data.movies:
        graph.run("MATCH (m:Movies),(d:Directors) WHERE m.title ={title} AND d.fullname ={director_name} CREATE (d)-[:DIRECTED]->(m)", title=movie.title,director_name=movie.director)
        for actor in movie.actors:
            graph.run("MATCH (m:Movies),(a:Actors) WHERE m.title ={title} AND a.fullname ={actor_name} CREATE (a)-[:ACTED_IN]->(m)", title=movie.title,actor_name=actor)

    for i in xrange(len(collect_lines)):
        line=collect_lines[i].strip().split('%')
        graph.run("MATCH (c:Collectors),(m:Movies) WHERE c.userid ={id} AND m.mov_id ={id2} CREATE (c)-[:COLLECTS]->(m)", id=line[0],id2=line[1])

    for i in xrange(len(follow_lines)):
        print i
        line=follow_lines[i].strip().split('%')
        graph.run("MATCH (c:Collectors),(c2:Collectors) WHERE c.userid ={id} AND c2.userid ={id2} CREATE (c)-[:FOLLOWS]->(c2)", id=line[0],id2=line[1])

def main():
    args=arg_parser()
    data,collect_lines,follow_lines=read_files(args)
    graph=neo4j(args.user,args.password,args.hostname,data)
    relation(data,graph,collect_lines,follow_lines)

if __name__ == '__main__':
    main()
