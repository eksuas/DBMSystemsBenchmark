# -*- coding: utf-8 -*-
import sys
from py2neo import authenticate, Graph,Node
import argparse
from sets import Set

class Movie:
    def __init__(self,line):
        self.line=line
    def movie_node(self,line):
        graph=Graph()
        start=graph.begin()
        # take needed info from line
        id=line[0],
        title=line[1],
        year=line[2],
        genre=line[4],
        director=line[5],
        rating=line[6],
        # Create node for Movies by taking from movies and add properties into node
        node1=Node("Movies",
                   id=id,
                   title=title,
                   released_year=year,
                   rating=rating,
                   genre=genre)
        #Create it
        start.merge(node1)
        start.commit()
        # I returned node, maybe I can use to create relationship between nodes later.(I'm not sure now.)
        return node1


class Director:
    def __init__(self,line):
        self.line=line
    def director_node(self,director_set):
        graph=Graph()
        start=graph.begin()
        dnode_list=[]
        director_id=3000
        for director in director_set:
            # Node for directors in director_set
            node2 = Node("Directors",userid=director_id, fullname=director)
            start.merge(node2)
            #Add node into director node list to use later.
            dnode_list.append(node2)
            director_id+=1
        start.commit()
        return dnode_list


class Actor:
    def __init__(self,line):
        self.line=line
    def actor_node(self,actor_set):
        anode_list=[]
        graph=Graph()
        start=graph.begin()
        actor_id=2000
        for actor in actor_set:
            #Node for actors in actor_set
            node3=Node("Actors", userid=actor_id, fullname=actor)
            start.merge(node3)  #Create it
            # Add node into nodelist to return all of them
            anode_list.append(node3)
            actor_id+=1
        start.commit()
        # I returned node list, maybe I can use to create relationship between nodes later.
        return anode_list


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
        default="staj/data/FILMS.txt",
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
        movies=Movie(line)
        movie=movies.movie_node(line)
        #Add every new director into director_set
        director_set.add(line[5])
        actor_list=line[3].split(',')
        #Add every new actor in actor_list into actor_set
        for actor in actor_list:
            actor_set.add(actor)
    #Call Director class
    director=Director(line)
    # Send director_set into method in class to create node from it
    directors=director.director_node(director_set)
    #Call Actor class
    actor=Actor(line)
    #Send actor_set into method in class to create node from it
    actors=actor.actor_node(actor_set)
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
