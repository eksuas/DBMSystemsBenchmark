# -*- coding: utf-8 -*-
from py2neo import authenticate, Graph,Node

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--user', type=str, default="neo4j", help='username for neo4j')
parser.add_argument('--password', type=str, default="777", help='password of the user')

args = parser.parse_args()

authenticate("localhost:7474", args.user, args.password)
graph = Graph()
start=graph.begin()
#g=Graph("bolt://localhost:7687", auth=("neo4j", "777"))

#string="CREATE (Node:Actor{userid: "", fullname: ""})"

path=r"C:\Users\ASUS\Documents\staj\data\FILMS.txt"
f=open(path,"r")
actor_list=[]

text_lines=f.readlines()
i=0
print(len(text_lines))
while i<len(text_lines):
    line=text_lines[i].split(',')
    i+=1
    print(len(line))
    director=line[8]
    film_title=line[1]
    film_year=line[2]
    film_genre=line[7]
    film_rating=line[9]
    actor_list=[line[3],line[4],line[5],line[6]]
    node1 = Node("Directors",userid=i, fullname=director)
    node2=Node("Movies",id=i, title=film_title, released_year=film_year, rating=film_rating, genre=film_genre)
    start.create(node1)
    start.create(node2)
    for actor in actor_list:
        node3=Node("Actors", userid=i, fullname=actor)
        start.create(node3)


    start.commit()




"""
count=2
while count!=1:
    oku=f.readline()
    print(oku)
    count+=1

    other_lines=f.readline()
    line2=other_lines.split(',')
    length2=len(line2)
    other_diectors=line2[length2-2]
    print(other_diectors)
    node2 = Node("Director",userid=count, fullname=other_diectors)
    start.create(node2)
    count+=1
start.commit()
#    b=satir.split(',')
#    length=len(b)
        #print(length)
#    author=b[length-2]
    #    print(b[length-2])
    #st="MATCH (Director { userid:1}) SET Director += { userid:count , fullname:other_diectors }"
#    graph.run(st)

    #    c = Node("Actor",userid=i, fullname=author)
    #    c.push()




    c=graph.data("MATCH (a:Actor) RETURN a.userid, a.fullname LIMIT 4")
    print(c)
    print("************************************")
    d=graph.run("MATCH (a:Actor) RETURN a.userid, a.fullname LIMIT 4").data()
    print(d)
    print("************************************")
    e=graph.run("MATCH (a) WHERE a.userid={x} RETURN a.fullname", x=4).evaluate()
    print(e)
    #graph.run(MATCH (n) RETURN (n))
    i+=1





#oku=f.read()
#print(oku)



#for satir in open("FILMS.txt","r"):
    #print(satir)
"""
