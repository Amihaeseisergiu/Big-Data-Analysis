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


def run_with_percentage_of_data(path, percentage):
    lines = open(path, "r").readlines()
    lines = lines[:int(len(lines) * percentage / 100)]
    filename = f"./temp/test{time.time()}.txt"
    open(filename, "w").writelines(lines)
    conf = SparkConf().setMaster("local").setAppName("Apriori")
    sc = SparkContext(conf=conf)

    # Construct Apriori
    apriori = Apriori(filename, sc, minSupport=100)
    print("Loaded data. Starting fitting!")
    supports = apriori.fit()
    for i in supports.collect():
        print(i)
    print("Gata")


def main():
    # run_with_percentage_of_data("../data/msnbc/data.csv", 1)  # Works
    # run_with_percentage_of_data("../data/msnbc/data.csv", 10)  # Works
    run_with_percentage_of_data("../data/msnbc/data.csv", 50)  #


if __name__ == '__main__':
    main()
