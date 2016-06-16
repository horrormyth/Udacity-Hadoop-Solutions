__author__ = 'devndraghimire'

from mrjob.job import MRJob


class hits_Total(MRJob):

    def mapper(self, _, line):
        field=line.split()
        if field[0]=='10.99.99.186':
            yield 'hits',field[0]
    def reducer(self, key, values):
        i,totalhits = 0,0
        for i in values:
            totalhits+=1
        yield 'totalhits',totalhits

if __name__=='__main__':
    hits_Total.run()