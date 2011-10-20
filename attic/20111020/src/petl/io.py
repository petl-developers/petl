"""
TODO doc me

"""
import csv
import cPickle as pickle


def writecsv(table, path, *args, **kwargs):
    with open(path, 'wb') as file:
        writer = csv.writer(file, *args, **kwargs)
        writer.writerows(table)
        
        
class readcsv(object):
    
    def __init__(self, path, *args, **kwargs):
        self.path = path
        self.args = args
        self.kwargs = kwargs
        
    def __iter__(self):
        with open(self.path, 'rb') as file:
            reader = csv.reader(file, *self.args, **self.kwargs)
            for row in reader:
                yield row
                
                
def writepickle(table, path, protocol=-1):
    with open(path, 'wb') as file:
        for row in table:
            pickle.dump(row, file, protocol)
            
            
class readpickle(object):
    
    def __init__(self, path):
        self.path = path
        
    def __iter__(self):
        with open(self.path, 'rb') as file:
            try:
                while True:
                    yield pickle.load(file)
            except EOFError:
                pass
        
