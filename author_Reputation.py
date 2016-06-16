__author__ = 'devndraghimire'

# Find the reputation of the author from the post

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

REGEX = re.compile(r'\b([0-9]+)\b')

class reputation(MRJob):

    def configure_options(self):
        super(reputation,self).configure_options()
        self.add_file_option('--items',help='Path to userdata')

    def steps(self):
        return [
            MRStep(mapper=self.author_id_mapper,
                   reducer=self.authorid_reducer),
            MRStep(mapper= self.mapper_comparison,
                   mapper_init = self.load_id_rep,
                   reducer = self.final_reducer)
        ]

    # Sort out the authorids
    def author_id_mapper(self,_,line):
        data = line.split('\t')
        if len(data) == 19:
            authorids = data[3].strip('"')
            if(authorids!='author_id'):
                yield authorids,1

    # Reduce the authorid
    def authorid_reducer(self, authorid, values):
        # print authorid
        yield authorid,None

    # sort out the author id withe the reputation from the dictionary
    def mapper_comparison(self,authorid,nonkey):
        reputation = self.userData[authorid]
        # print reputation,authorid
        yield None,(authorid,reputation)

    # Display the proper values along witht the data

    def final_reducer(self, key, values):
        for x,y in values:
            yield x,y

# D
    def load_id_rep(self):
        self.userData ={}
        with open('forum_users.tsv') as file:
            for line in file:
                fields = line.split('\t')
                if len(fields)==5:
                    userid = fields[0].strip('"')
                    reputation = fields[1].strip('"')
                    self.userData[userid] = reputation
                    # print self.userData


########################################
# This works with the minimiz forum, need to rip off the author_id from the first line

if __name__ == '__main__':
    reputation.run()
