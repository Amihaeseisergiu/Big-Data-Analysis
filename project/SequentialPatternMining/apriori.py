import sys
import time

import numpy as np
from pyspark import SparkConf, SparkContext

from SequentialPatternMining.apriori_pyspark.papriori import Apriori


def load_data_non_spark(path="./data/msnbc/data.seq"):
    lines = open(path, "r").readlines()
    lines = map(str.split, lines)
    lines = tuple(map(lambda string: tuple(map(int, string)), lines))
    return lines


def print_as_csv(data):
    import csv
    with open("../data/msnbc/data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)


def main():
    conf = SparkConf().setMaster("local").setAppName("Apriori")
    sc = SparkContext(conf=conf)

    path = "../data/msnbc/data.csv"

    # Construct Apriori
    apriori = Apriori(path, sc, minSupport=2)
    supports = apriori.fit()
    print(supports.collect())


if __name__ == '__main__':
    # conf = SparkConf().setMaster("local").setAppName("George's K-Means")
    # sc = SparkContext(conf=conf)
    main()
