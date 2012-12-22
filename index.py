import os

import enron

if __name__ == '__main__':
    enron.process_files(os.path.join(enron.DOCUMENT_PATH))
