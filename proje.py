# -*- coding: utf-8 -*-
import sys
from py2neo import authenticate, Graph,Node
import argparse
from sets import Set

def Arg_Parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user',   ## Add user argument for server
    type=str,
    default="neo4j",
    help='username for neo4j')
    parser.add_argument('--password',   #Add password argument for server
    type=str,
    default="777",
    help='password of the user')
    parser.add_argument('--path',       #Add file path argument for FILMS.txt file
    default="data/FILMS.txt",
    help='the path of FILMS.txt file')
    try:
        args = parser.parse_args()

    except SystemExit as e:
        print(e)
        sys.exit()
    return args      # Return user,password,path arguments to use in other functions

def file_operations(args):
    try:
        f=open(args.path,"r") # Open file with args.path
    except IOError:
        print("The file is not found.")  #if there is a error, do it
        sys.exit()

    text_lines=f.readlines()  # Read file
    actor_set=Set()
    director_set=Set()
    i=0
    while i<len(text_lines):        # to read every line

        line=text_lines[i].split('%')
        i+=1
        Movie_dict={              # Create dictionary for movies
        'film_id':line[0],
        'film_title':line[1],
        'film_year':line[2],
        'actor_list':line[3].split(','),  # Divide actors(line[3]=actors) incording to "," and add into actor_list
        'film_genre':line[4],
        'director':line[5],
        'film_rating':line[6],
        }
        Neo4j_movie(Movie_dict,args)  #Send every movie_dict into Neo4j_movie() function to create node
        director_set.add(line[5])     #Add every new director into director_set
        for actor in Movie_dict['actor_list']:    #Add every new actor in actor_list into actor_set
            actor_set.add(actor)

    return Movie_dict,director_set,actor_set

def Neo4j_movie(Movie_dict,args): # Neo4j operation of Movies

    try:
        authenticate ("localhost:7474", args.user, args.password)   # Authenticate for server and connect
        graph=Graph()
    except Exception as e:    # If server is not connected
        print ("Unable to reach server.")
        sys.exit()
    start=graph.begin()
    node1=Node("Movies",            # Create node for Movies by taking from Movie_dict
        id=Movie_dict['film_id'],        # And add properties into node
        title=Movie_dict['film_title'],
        released_year=['film_year'],
        rating=['film_rating'],
        genre=['film_genre'])
    start.merge(node1)   #Create it
    start.commit()


def Neo4j(director_set,actor_set,args): #Neo4j operation of director and actors

    try:
        authenticate ("localhost:7474", args.user, args.password)   # Authenticate for server and connect
        graph=Graph()
    except Exception as e:    # If server is not connected
        print ("Unable to reach server.")
        sys.exit()
    director_id=3000
    actor_id=2000
    start=graph.begin()
    for director in director_set:
        node2 = Node("Directors",userid=director_id, fullname=director)  # Node for directors in director_set
        start.merge(node2)   #Create it
        director_id+=1

    for actor in actor_set:
        node3=Node("Actors", userid=actor_id, fullname=actor)    #Node for actors in actor_set
        start.merge(node3)  #Create it
        actor_id+=1

    start.commit()   # Create all nodes in server



def main():#main function
    args=Arg_Parser() # first function
    arguments=file_operations(args)  # Call second function and put results(Movie_dict,
#director_set,actor_set) into arguments
    Neo4j(arguments[1],arguments[2],args) #Third function for  neo4j operations of directors and actors

if __name__ == '__main__':
    main()
