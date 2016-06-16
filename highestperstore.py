__author__ = 'devndraghimire'
from mrjob.job import MRJob

class highest_Individual_PersStore(MRJob):
    def mapper(self,_,line):
        (date,time,store,category,cost,paytype) = line.split('\t')
        yield store,float(cost)

    def reducer(self, store, cost):
        yield store,"%0.2f"%max(cost)

if __name__ == '__main__':
    highest_Individual_PersStore.run()