class Utils:
    @staticmethod
    def euclideanDistance(point1, point2):
        if isinstance(point1, list):
            summation = 0

            for i in range(len(point1)):
                summation += (point1[i] - point2[i]) ** 2
            
            return summation
        else:
            return (point1 - point2) ** 2

    @staticmethod
    def minus(vector1, vector2):
        vector = []

        for i in range(len(vector1)):
            vector.append(vector1[i] - vector2[i])

        return vector

    @staticmethod
    def div(vector1, scalar):
        vector = []

        for i in range(len(vector1)):
            vector.append(vector1[i] / scalar)

        return vector

    @staticmethod
    def plus(vector1, vector2):
        vector = []

        for i in range(len(vector1)):
            vector.append(vector1[i] + vector2[i])

        return vector

    @staticmethod
    def dot(vector1, vector2):
        result = 0

        for i in range(len(vector1)):
            result += vector1[i] * vector2[i]
        
        return result

    @staticmethod
    def toFloatArray(array):
        return [float(x) for x in array]