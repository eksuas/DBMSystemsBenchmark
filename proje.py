# -*- coding: utf-8 -*-
import sys
from py2neo import authenticate, Graph,Node
import argparse
from sets import Set

# Command line argument parser
def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user',
        type=str,
        default="neo4j",
        help='username for neo4j')
    parser.add_argument('--password',
        type=str,
        default="777",
        help='password of the user')
    parser.add_argument('--hostname',
        default="localhost:7474",
        help='hostname of the server')
    parser.add_argument('--path',
        default="data/FILMS.txt",
        help='the path of FILMS.txt file')
    try:
        args = parser.parse_args()

    except SystemExit as e:
        print(e)
        sys.exit()
    # Return user,password,path arguments to use in other functions
    return args

def file_operations(path):
    try:
        # Open file with args.path
        f=open(path,"r")
    except IOError:
         #if there is a error, do it
        print("The file is not found.")
        sys.exit()
    # Read file
    text_lines=f.readlines()
    return(text_lines)

 #Neo4j operations :
def neo4j(user,password,hostname,text_lines):
    try:
        # Authenticate for server and connect it
        authenticate (hostname, user, password)
        graph=Graph()
    # If server is not connected :
    except Exception as e:
        print ("Unable to reach server.")
        sys.exit()
    start=graph.begin()
    actor_set=Set()
    director_set=Set()
    i=0
    while i<len(text_lines):
        line=text_lines[i].split('%')
        i+=1
        # Create dictionary for movies
        movies={
            'id':line[0],
            'title':line[1],
            'year':line[2],
            # Divide actors(line[3]=actors) incording to "," and add into actor_list
            'actor_list':line[3].split(','),
            'genre':line[4],
            'director':line[5],
            'rating':line[6],
        }
      # Create node for Movies by taking from movies and add properties into node
        node1=Node("Movies",
                id=movies['id'],
                title=movies['title'],
                released_year=['year'],
                rating=['rating'],
                genre=['genre'])
        #Create it
        start.merge(node1)
        #Add every new director into director_set
        director_set.add(line[5])
        #Add every new actor in actor_list into actor_set
        for actor in movies['actor_list']:
            actor_set.add(actor)

    director_id=3000
    actor_id=2000
    for director in director_set:
        # Node for directors in director_set
        node2 = Node("Directors",userid=director_id, fullname=director)
        start.merge(node2)   #Create it
        director_id+=1

    for actor in actor_set:
        #Node for actors in actor_set
        node3=Node("Actors", userid=actor_id, fullname=actor)
        start.merge(node3)  #Create it
        actor_id+=1
     # Create all nodes in server
    start.commit()



#main function
def main():
    # first function
    args=arg_parser()
    # Call second function and put results(movies, director_set,actor_set) into arguments
    text_lines=file_operations(args.path)
    #Third function for  neo4j operations of directors and actors
    neo4j(args.user,args.password,args.hostname,text_lines)
if __name__ == '__main__':
    main()
