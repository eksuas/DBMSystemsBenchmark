# -*- coding: utf-8 -*-
import dataset
import argparse
from py2neo import authenticate, Graph,Node,Relationship,NodeSelector
from sets import Set
import datetime
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

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

#Neo4j operations
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
    questions_list.append("1. List all actors ( userid and fullname ) who are also directors")
    questions_list.append("2. List all actors ( userid and fullname ) who acted in 5 or more movies.")
    questions_list.append("3. How many actors have acted in same movies with ’Edward Norton’?")
    questions_list.append("4. Which collectors collected all movies in which ’Edward Norton’ acts?")
    questions_list.append("5. List 10 collectors ( userid and fullname ) who collect ’The Shawshank Redemption’.")
    questions_list.append("6. List all userids and fullnames of xi’s which satisfy Degree(1001, xi) ≤ 3")
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

def main():
    args=arg_parser()
    data=read_files(args)
    #Learn time difference for neo4j operations
    now = datetime.datetime.now()
    neo4j(args.user,args.password,args.hostname,data)
    end = datetime.datetime.now()
    print (end-now)

if __name__ == '__main__':
    main()
