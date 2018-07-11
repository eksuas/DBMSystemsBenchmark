import dataset
import argparse
import sys
from sets import Set

# Command line argument parser
def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user',            default="neo4j",                help='username for neo4j')
    parser.add_argument('--password',        default="777",                  help='password of the user')
    parser.add_argument('--hostname',        default="localhost:7474",       help='hostname of the server')
    parser.add_argument('--films_path',      default="data/FILMS.txt",       help='the path of FILMS.txt file')
    parser.add_argument('--collectors_path', default="data/collectors.txt",  help='the path of collectors.txt file')
    try:
        args = parser.parse_args()
    except SystemExit as e:
        print(e)
        sys.exit()
        
    return args

def read_files(args):
    try:
        # Open files
        films_file=open(args.films_path,"r")
        collectors_file=open(args.collectors_path,"r")

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
        movie.actors=Set(line[3].split(', '))
        data.movies.add(movie)
        data.actors.union(movie.actors)
        data.directors.add(movie.director)

    # Assign id to actors and directors
    data.actors=Set([dataset.Actor(ID+2000, name) for ID, name in enumerate(data.actors]))
    data.directors=Set([dataset.Director(ID+3000, name) for ID, name in enumerate(data.directors])])

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
    return data

def main():
    args=arg_parser()
    data=read_files(args)
    for a in data.directors:
        print(a.ID)
        print(a.name)

if __name__ == '__main__':
    main()
