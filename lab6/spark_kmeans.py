from pyspark import SparkConf, SparkContext
from utils import Utils

class SparKMeans:
    def __init__(self, sc: SparkContext):
        self.sc = sc

    @staticmethod
    def closestCentroid(point, centroids):
        closest_centroid_index = 0
        closest_distance = float("inf")

        for centroid_index, centroid in enumerate(centroids):
            distance = Utils.euclideanDistance(point, centroid)

            if distance < closest_distance:
                closest_distance = distance
                closest_centroid_index = centroid_index

        return closest_centroid_index

    @staticmethod
    def have_centroids_changed(old_centroids, new_centroids_data):
        centroid_distances_difference = 0

        for centroid_index, new_centroid in new_centroids_data:
            centroid_distances_difference += Utils.dot(
                Utils.minus(old_centroids[centroid_index], new_centroid),
                Utils.minus(old_centroids[centroid_index], new_centroid),
            )

        return centroid_distances_difference > 0

    @staticmethod
    def copy_centroids(new_centroids_data):
        centroids = []

        for _, centroid in new_centroids_data:
            centroids.append(centroid)

        return centroids

    def fit(self, K, input_file, output_dir):
        file = self.sc.textFile(input_file)

        points = file.map(lambda line: Utils.toFloatArray(line.split(','))).cache()
        centroids = points.takeSample(withReplacement=False, num=K)

        centroids_change = True

        while centroids_change:
            new_centroid_data = points.map(
                    lambda point: (SparKMeans.closestCentroid(point, centroids), (point, 1))
                ).reduceByKey(
                    lambda centroid1, centroid2: (Utils.plus(centroid1[0], centroid2[0]), centroid1[1] + centroid2[1])
                ).map(
                    lambda centroid: (centroid[0], Utils.div(centroid[1][0], centroid[1][1]))
                ).collect()

            if not SparKMeans.have_centroids_changed(centroids, new_centroid_data):
                centroids_change = False

            centroids = SparKMeans.copy_centroids(new_centroid_data)

        return centroids


            