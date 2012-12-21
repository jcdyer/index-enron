import sys

import enron

if __name__ == '__main__':
    search_terms = sys.argv[1:]
    for result in enron.search(search_terms):
        print result


