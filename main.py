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
        # collectors_file=open(args.collectors_path,"r")
    except IOError:
         #if there is a error, do it
        print("The files are not found.")
        sys.exit()

    # Create data instance
    data = dataset.Data()

    # Read Films file
    lines=films_file.readlines()

    for i in xrange(len(lines)):
        line=lines[i].split(' % ')
        # Create dictionary for movies
        movie = dataset.Movie(
            id=line[0],
            title=line[1],
            year=line[2],
            genre=line[4],
            director=line[5],
            rating=line[6]
        )
        # Divide actors(line[3]=actors) incording to "," and add into actor_list
        movie.actors = Set(line[3].split(', '))
        data.movies.add(movie)

    return data


def main():
    args=arg_parser()
    data=read_files(args)
    for m in data.movies:
        print m.title

if __name__ == '__main__':
    main()
