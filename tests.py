import unittest

import os
import subprocess
import indexer

class NodeTestCase(unittest.TestCase):
    def setUp(self):
        indexer.DATA_DIR = os.path.abspath('test_data')
        os.mkdir(indexer.DATA_DIR)

    def tearDown(self):    
        subprocess.call(['rm', '-r', indexer.DATA_DIR])
        
    def test_insert_entry(self):
        indexer.insert_entry('word', 'test/file')
        index_node = os.path.join(indexer.DATA_DIR, 'w.dat')
        self.assertTrue(os.path.exists(index_node))
        with open(index_node) as node:
            self.assertEqual(node.readline(), 'word\ttest/file\n')
        
    def test_split_noe(self):
        index_node = os.path.join(indexer.DATA_DIR, 'w.dat')
        split_node_1 = os.path.join(indexer.DATA_DIR, 'w/a.dat')
        split_node_2 = os.path.join(indexer.DATA_DIR, 'w/o.dat')
        indexer.insert_entry('word', 'test/file')
        indexer.insert_entry('wart', 'test/file')
        self.assertTrue(os.path.exists(index_node))
        indexer.split_node(index_node)
        self.assertFalse(os.path.exists(index_node))
        self.assertTrue(os.path.exists(split_node_1))
        self.assertTrue(os.path.exists(split_node_2))
 
    def test_get_node_parts(self):
        path = os.path.join(indexer.DATA_DIR, 'a', 'b', 'c.dat')
        self.assertEqual(indexer.get_indexpath_parts(path), ['a', 'b', 'c'])
 
    def test_match_files(self):
        files = ['1.', '43.', '876', '2se.', '.', '6543.']
        self.assertEqual(['1.', '43.', '6543.'], list(indexer.match_files(files)))
    

class InsertionTestCase(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
