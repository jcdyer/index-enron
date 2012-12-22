
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
            self.pool[filename].close()
            del self.pool[filename]
