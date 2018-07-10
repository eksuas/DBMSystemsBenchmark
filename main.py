import dataset
import argparse
import sys
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
    parser.add_argument('--films_path',
        default="data/FILMS.txt",
        help='the path of FILMS.txt file')
    parser.add_argument('--collectors_path',
        default="data/collectors.txt",
        help='the path of collectors.txt file')
    try:
        args = parser.parse_args()

    except SystemExit as e:
        print(e)
        sys.exit()
    # Return user,password,path arguments to use in other functions
    return args


def read_files(args):
    try:
        # Open file with args.path
        films_file=open(args.films_path,"r")
        collectors_file=open(args.collectors_path,"r")
    except IOError:
         #if there is a error, do it
        print("The files are not found.")
        sys.exit()

    # Create data instance
    data = dataset.Data()

    # Read files
    films_lines=films_file.readlines()
    collectors_lines=collectors_file.readlines()
    collector_id=1000
    director_id=3000
    actor_id=2000
    actor_set=Set()
    director_set=Set()
    # Firstly, read informations in FILMS file
    for i in xrange(len(films_lines)):

        line=films_lines[i].split(' % ')
        actor_list=(line[3].split(', '))
        #Add directors of movie into director set
        director_set.add(line[5])
        #Add actors of the movie into set
        for actor in actor_list:
            actor_set.add(actor)
        #Send movie informations to dataset and create a movie
        movie = dataset.Movie(
            ID=line[0],
            title=line[1],
            year=line[2],
            genre=line[4],
            director=line[5],
            rating=line[6]
        )
        #Add movie and into movies set in dataset
        data.movies.add(movie)
        #Send informations of every director to Director class in dataset
    for director in director_set:
        #Create director
        director=dataset.Director(
            ID=director_id,
            name=director
        )
        director_id+=1
        # Add director into directors set in dataset
        data.directors.add(director)


    #Send every actor to Actor class in dataset
    for actor in actor_set:
        #Create actor
        actor=dataset.Actor(
            ID=actor_id,
            name=actor

        )
        actor_id+=1

        # Add actor into actors set in datasetc
        data.actors.add(actor)

    #Read lines in collectors file
    for i in xrange(len(collectors_lines)):
        line=collectors_lines[i].split('%')
        #Send every collector to Collector class in dataset
        collector = dataset.Collector(
            ID=line[0],
            name=line[1],
        )
        #Add collector into collectors set in data class
        data.collectors.add(collector)

    return data


def main():
    args=arg_parser()
    data=read_files(args)


if __name__ == '__main__':
    main()
