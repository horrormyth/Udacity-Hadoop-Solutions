__author__ = 'devndraghimire'

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

REGEX = re.compile(r'\b([a-zA-Z0-9]+)\b')
TEXT = 'fantastically'

class fantastic_Search(MRJob):



    def mapper(self,_,line):
        data = line.strip().split('\t')
        if len(data)==19:
            nodeid = data[0]
            nodeid =  REGEX.findall(nodeid)
            text = data[4]
            # bodytext = re.split(r'[,.!?:;"()<>\[\]#$=\-/\s\t]', text)
            bodytext = REGEX.findall(text)

            for word in bodytext:
                word = word
                lower = word.lower()
                # check if the text appears in text if yes return it
                if TEXT in lower:
                    yield None,(lower,nodeid)


    def reducer(self, list, values):
        x =[]
        count =0
        # loop in to get the fatastically nodeid vlaues
        for word,nodeid in values:
            x.append(nodeid)
            count+=1
        yield 'Fantastically = ',(count,x)
if __name__ == '__main__':
    fantastic_Search.run()
        # data[0],data[4]
