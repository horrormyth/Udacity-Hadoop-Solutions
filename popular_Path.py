__author__ = 'devndraghimire'

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class popular_Path(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper,reducer = self.reducer),
            MRStep(reducer =self.max_finder)
        ]
    def mapper(self, _, line):
        path=''
        data = line.split()
        if len(data)==10:
            a,b,c,d,e,f,paths,h,i,j=data
            if paths.startswith('http://www.the-associates.co.uk') == False:
                path = paths
                print path
            # else:
            #     path = re.sub(r"^http://www.the-associates.co.uk", '', paths)
            yield path,1
    def reducer(self, key, values):
        yield None,(key,sum(values))

    def max_finder(self,key,values):
        yield max(values)

if __name__=='__main__':
    popular_Path.run()