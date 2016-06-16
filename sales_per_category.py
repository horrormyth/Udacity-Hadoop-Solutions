__author__ = 'devndraghimire'
from mrjob.job import MRJob


class salesPerCategory(MRJob):

    def counter_mapper(self, _ , line):
        (date,time,place,category,cost,paytype)= line.split('\t')
        yield category,float(cost)

    def reducer(self,category,cost):
        yield category,"%0.2f"%sum(cost)

if __name__ == '__main__':
    salesPerCategory.run()

