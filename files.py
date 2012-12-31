from contextlib import contextmanager
from io import BytesIO

def buffered_reader(filename, chunk_size=2**20):
    with open(filename) as index:
        index_buffer = BytesIO()
        loc = 0
        end = 0
        chunk = index.read(chunk_size)
        print "chunk", chunk
        index_buffer.write(chunk)
        end += len(chunk)
        index_buffer.seek(0)
        while 1:
            line = index_buffer.readline()
            loc += len(line)
            if len(line) == 0:
                break
            if loc == end:
                #The buffer is empty.  Get more, if we can.
                chunk = index.read(chunk_size)
                index_buffer.write(chunk)
                index_buffer.seek(loc)
                end += len(chunk)
                if not line.endswith('\n'):
                    # We may not have gotten a full line
                    more_line = index_buffer.readline()
                    line = line + more_line
                    loc += len(more_line)
            yield line

class FilePool(object):
    
    def __init__(self, limit):
        self.limit = limit
        self.pool = {}

    @property
    def size(self):
        return len(self.pool)

    def write(self, filename, content):
        if filename not in self.pool:
            if self.size >= self.limit:
                self.clear_pool()
            self.pool[filename] = open(filename, 'a')
        self.pool[filename].write(content)

    def clear_pool(self):
        for filename in self.pool.keys():
            self.close(filename)

    def close(self, filename):
        if filename in self.pool:
            self.pool[filename].close()
            del self.pool[filename]
