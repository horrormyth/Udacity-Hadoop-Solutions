__author__ = 'devndraghimire'
from mrjob.job import MRJob
from mrjob.step import MRStep


class total_Sales(MRJob):

    # def steps(self):
    #     return [
    #         MRStep(mapper =self.mapper,reducer = self.reducer),
    #         MRStep(reducer = self.reduce_counter)
    #     ]
    def mapper(self,_,line):
        (date,time,place,category,costs,paytype)= line.split('\t')
        yield 'cost',float(costs)

    def reducer(self, key, values):
        # loop over all the values and sum it up, and count
        x,total_count,total_amount =0,0,0.0
        for x in values:
            total_count+=1
            total_amount+=x
        yield 'Count and Total',(total_count,"%0.2f"%total_amount)


        # yield category,sum(count)
if __name__ == '__main__':
    total_Sales.run()

