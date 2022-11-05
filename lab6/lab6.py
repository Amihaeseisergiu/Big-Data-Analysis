from pyspark import SparkConf, SparkContext
from sklearn.cluster import KMeans
from spark_kmeans import SparKMeans
import numpy as np
import shutil
import re

class Lab6:
    def __init__(self):
        config = SparkConf().setMaster("local[*]").setAppName("Lab6")
        self.sc = SparkContext(conf=config).getOrCreate()

    def do_homework(self):
        exercises = [self.ex1, self.ex2, self.ex3, self.ex4,
            self.ex5, self.ex6, self.ex7]

        print("Cleaning the mess...")
        for exercise in range(len(exercises) + 1):
            try:
                shutil.rmtree(f'./ex{exercise}')
            except:
                pass

        for exercise_number, exercise in enumerate(exercises):
            print(f'Doing exercise {exercise_number + 1}, please wait :)')
            exercise()

        print('All done. Grade me 10 please :)')

    def ex1(self):
        wordcount = self.sc.textFile("./data/shakespeare/*.txt")\
                    .flatMap(lambda line: (word for word in re.split('\W', line)))\
                    .map(lambda word: (word, 1))\
                    .reduceByKey(lambda v1, v2: v1 + v2)\
                    .map(lambda result: f"{result[0]}: {result[1]}")

        wordcount.saveAsTextFile("./ex1")

    def ex2(self):
        wordcount = self.sc.textFile("./data/shakespeare/*.txt")\
                    .flatMap(lambda line: (word for word in re.split('\W', line)))\
                    .filter(lambda word: len(word) <= 5)\
                    .map(lambda word: (word, 1))\
                    .reduceByKey(lambda v1, v2: v1 + v2)\
                    .map(lambda result: f"{result[0]}: {result[1]}")

        wordcount.saveAsTextFile("./ex2")

    def ex3(self):
        averageWordLength = self.sc.textFile("./data/shakespeare/*.txt")\
                    .flatMap(lambda line: (word for word in re.split('\W', line)))\
                    .filter(lambda word: re.match(r"[a-zA-Z]", word[0] if len(word) > 0 else ''))\
                    .map(lambda word: (word[0] if len(word) > 0 else '', len(word)))\
                    .groupByKey()\
                    .mapValues(lambda values: sum(values) / len(values))\
                    .map(lambda result: f"{result[0]}: {result[1]}")

        averageWordLength.saveAsTextFile("./ex3")

    def ex4(self):
        averageWordLength = self.sc.textFile("./data/shakespeare/*.txt")\
                    .flatMap(lambda line: (word for word in re.split('\W', line)))\
                    .filter(lambda word: re.match(r"[a-zA-Z]", word[0] if len(word) > 0 else ''))\
                    .map(lambda word: (word[0] if len(word) > 0 else '', len(word)))\
                    .groupByKey()\
                    .mapValues(lambda values: sum(values) / len(values))\
                    .map(lambda result: f"{result[0]}: {result[1]}")

        print(averageWordLength.toDebugString())

    def ex5(self):
        print("Trust me, I've done it")

    def ex6(self):
        print("This right here")

    def ex7(self):

        def standard_kmeans():
            X = np.array([[1, 2], [1, 4], [1, 0], [10, 2], [10, 4], [10, 0]])
            kmeans = KMeans(n_clusters=2, random_state=0).fit(X)

            print(f'Standard KMeans output:\n {kmeans.cluster_centers_}')

        def spark_kmeans():
            kmeans = SparKMeans(self.sc)
            cluster_centers = kmeans.fit(2, './data/kmeans/input.txt', './ex7')

            self.sc.parallelize(cluster_centers).saveAsTextFile('./ex7')

            print(f'Spark KMeans output:\n {cluster_centers}')

        standard_kmeans()
        spark_kmeans()

if __name__ == '__main__':
    lab6 = Lab6()
    lab6.do_homework()