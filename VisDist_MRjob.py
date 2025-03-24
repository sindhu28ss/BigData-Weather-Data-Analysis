from mrjob.job import MRJob
import re
import sys


class MRVisibilityDistance(MRJob):

    def mapper(self, _, line):
        val = line.strip()
        usaf_id = val[4:10]
        visibility = val[78:84]
        quality_code = val[84:85]

        # Filter invalid visibility values and bad quality codes
        if visibility != "999999" and re.match("[01459]", quality_code):
            yield (usaf_id, int(visibility))

    def reducer(self, usaf_id, visibilities):
        for visibility in visibilities:
            yield (usaf_id, visibility)


if __name__ == "__main__":
    MRVisibilityDistance.run()
