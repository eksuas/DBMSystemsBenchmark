from sets import Set

class Data:
    def __init__(self):
        self.actors = Set()
        self.directors = Set()
        self.movies = Set()
        self.collectors = Set()

class Movie:
    def __init__(self,ID,title,year,genre,director,rating):
        self.ID = ID
        self.title = title
        self.year = year
        self.genre = genre
        self.directors = Set()
        self.rating = rating
        self.actors = Set()
        self.collectors = Set()

class Actor:
    def __init__(self,ID,name):
        self.ID = ID
        self.name=name

class Director:
    def __init__(self,ID,name):
        self.ID = ID
        self.name = name
        
class Collector:
    def __init__(self,ID,name,email):
        self.ID = ID
        self.name = name
        self.email=email
