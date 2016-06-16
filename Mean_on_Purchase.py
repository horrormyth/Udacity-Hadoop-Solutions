__author__ = 'devndraghimire'


# Is there any correlation between the day of the week
# and how much people spend on the items

from mrjob.job import MRJob
from datetime import datetime
import numpy as np

class weekday_Mean(MRJob):

    def mapper(self, _, line):
        (date,time,city,Store,amount,paytype)= line.split('\t')
        weekday = datetime.strptime(date,'%Y-%m-%d').weekday()

        yield None,(weekday,float(amount))

    def reducer(self, weekday,amounts):
        count = 0
        total_amount =0.0
        for week,amnt in amounts:
            count+=1
            total_amount = total_amount+amnt
        yield week,'%0.2f'%(total_amount/count)



if __name__ =='__main__':
    weekday_Mean.run()

