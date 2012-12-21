#!/usr/bin/env/python

import os
import re
import subprocess
import sys

DOCUMENT_PATH=os.path.join(os.environ['HOME'], 'Desktop', 'skilling-j')
DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
MAX_NODE_SIZE = 2**16

file_re = re.compile(r'^[0-9]+\.$')

def match_files(files):
    for filename in files:
        if file_re.match(filename):
            yield filename

def process_files(root):
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
    for dir, subdirs, files in os.walk(root):
        for i, file in enumerate(match_files(files)):
            sys.stdout.write('.')
            if i % 500 == 0:
                sys.stdout.write('%i' % i)
            sys.stdout.flush()
            filepath = os.path.join(dir, file)
            index_email(filepath)
            
def find_node(word):
    wordpath = DATA_DIR
    for letter in word:
        wordpath = os.path.join(wordpath, letter)
        if os.path.isdir(wordpath):
            continue
        else:
            return wordpath + '.dat'
    if wordpath == DATA_DIR:
        #No indexable letters
        return None
    return wordpath + '.dat'
    
def get_indexpath_parts(indexpath):
    indexpath = os.path.abspath(indexpath)
    if indexpath.startswith(DATA_DIR):
        indexpath = indexpath[len(DATA_DIR):]
    indexpath = indexpath.lstrip('/')
    path_parts = indexpath.split(os.path.sep)
    if path_parts[-1].endswith('.dat'):
        path_parts[-1] = path_parts[-1][:-4]
    return path_parts
  
def pack_node(indexpath):
    return 
    # do something like:
    # $ sort $indexpath| uniq|outfile.tmp; 
    # $ cp outfile.tmp $indexpath

def split_node(indexpath):
    os.rename(indexpath, indexpath + '.old')
    parts = get_indexpath_parts(indexpath)
    newpath = os.path.join(DATA_DIR, *parts)
    if not os.path.exists(newpath):
        os.mkdir(newpath)
    partial_word = ''.join(parts)
    with open('%s.old' % indexpath) as index:
        for entry in index:
            word, email = read_entry(entry)
            insert_entry(word, email)
    os.unlink(indexpath + '.old')

def get_words(email):
    #skip headers
    for line in email:
        line = line.strip()
        if line == '':
            break
    words_seen = set()
    for line in email:
        for word in line.split():
            word = preprocess_word(word)
            if word and word not in words_seen:
                words_seen.add(word)
                yield word
    
def preprocess_word(word):
    return ''.join(c for c in word.lower() if c not in '><().,/;\'"!?[]{}\\|+=')

def read_entry(entry):
    return entry.strip().split('\t')
    
def insert_entry(word, filename):
    indexpath = find_node(word)
    if not indexpath:
        #Not an indexable word.
        return
    try:
        indexstat = os.stat(indexpath)
    except OSError:
        # Probably a new file
        pass
    else:
        # assuming this will pass because it did before, so 
        # the file exists.  This is not always a safe assumption.
        indexstat = os.stat(indexpath)
        if indexstat.st_size > MAX_NODE_SIZE:
            split_node(indexpath)
            indexpath = find_node(word)
    with open(indexpath, 'a') as index:
        index.write('%s\t%s\n' % (word, filename))

    
def index_email(filename):
    if filename.startswith(DOCUMENT_PATH):
        document_id = filename[len(DOCUMENT_PATH):]
    else:
        document_id = filename
    with open(filename) as email:
        for word in get_words(email):
            insert_entry(word, document_id)

def search(terms):
    seen_emails = set()
    for term in terms:
        search_target = DATA_DIR
    
        for c in preprocess_word(term):
            search_target = os.path.join(search_target, c)
            if not os.path.exists(search_target):
                break
        search_file = search_target + '.dat'
        if os.path.exists(search_file):
            # return exact results first
            for email in get_search_results(search_file, term):
                if email not in seen_emails:
                    seen_emails.add(email)
                    yield email
        if os.path.isdir(search_target):
            for dir, subdirs, files in os.walk(search_target):
                for file in files:
                    search_file = os.path.join(dir, file)
                    for email in get_search_results(search_file, term):
                        if email not in seen_emails:
                            seen_emails.add(email)
                            yield email

def get_search_results(search_file, term):
    with open(search_file) as index:
        for entry in index:
            word, email = read_entry(entry)
            if word.startswith(term):
                yield email

if __name__ == '__main__':
    #process_files(os.path.join(DOCUMENT_PATH, 'inbox'))
    while True:
        terms = raw_input('enter search terms')
        for result in search(terms.split()):
            print result


