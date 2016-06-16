__author__ = 'devndraghimire'

from mrjob.job import MRJob
from mrjob.step import MRStep


class topten_Hits(MRJob):

    def steps(self):
        return [

            MRStep(mapper= self.data_mapper,reducer = self.count_reducer),
            MRStep(reducer = self.sort_reducer)
        ]

    def data_mapper(self,_,line):
        data = line.split()
        if len(data)==10:
            ip,b,c,d,e,f,g,h,i,j = data
            yield ip,1

    def count_reducer(self,ip,count):
        yield None,(sum(count),ip)

    def sort_reducer(self,key,values):
        results = sorted(values, reverse=True)[:5]
        for count,ip in results:
            yield count,ip

if __name__ == '__main__':
    topten_Hits.run()