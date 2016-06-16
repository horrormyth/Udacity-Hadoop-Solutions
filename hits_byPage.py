__author__ = 'devndraghimire'

from mrjob.job import MRJob

class hits_byIP(MRJob):

    def mapper(self,_,line):
        string = '/assets/js/the-associates.js'
        # string = '/assets/js/lowpro.js'
        fields = line.split()
        if string in fields:
            yield 'page',1

    def reducer(self, key, values):
        i,total =0,0
        for i in values:
            total+=1
        yield 'Hits by Page',total
if __name__ =='__main__':
    hits_byIP.run()
