# Sum oparation for reduce by key
def sumOparator(x, y):
    return x + y


# Remove replications after cartesian oparation
def removeReplica(record):
    if isinstance(record[0], tuple):
        x1 = record[0]
        x2 = record[1]
    else:
        x1 = [record[0]]
        x2 = record[1]

    if not any(x == x2 for x in x1):
        a = list(x1)
        a.append(x2)
        a.sort()
        result = tuple(a)
        return result
    else:
        return x1


# Filter items
def filterForConf(item):
    if len(item[0][0]) > len(item[1][0]):
        if not checkItemSets(item[0][0], item[1][0]):
            pass
        else:
            return item
    else:
        pass


# Check Items sets includes at least one comman item // Example command: # any(l == k for k in z for l in x )
def checkItemSets(item_1, item_2):
    if len(item_1) > len(item_2):
        return all(any(k == l for k in item_1) for l in item_2)
    else:
        return all(any(k == l for k in item_2) for l in item_1)


# Confidence calculation
def calculateConfidence(item):
    # Parent item list
    parent = set(item[0][0])

    # Child item list
    if isinstance(item[1][0], str):
        child = set([item[1][0]])
    else:
        child = set(item[1][0])
    # Parent and Child support values
    parentSupport = item[0][1]
    childSupport = item[1][1]
    # Finds the item set confidence is going to be found

    support = (parentSupport / childSupport) * 100

    return list([list(child), list(parent.difference(child)), support])


class Apriori:

    def __init__(self, path, sc, minSupport=2):

        # File path
        self.confidences = None
        self.path = path

        # Spark Context
        self.sc = sc

        self.minSupport = minSupport
        self.raw = self.sc.textFile(self.path)

        ## Whole Date set with frequencies
        self.lblitems = self.raw.map(lambda line: line.split(','))

        ## Whole lines in single array
        self.wlitems = self.raw.flatMap(lambda line: line.split(','))

        ## Unique frequent items in dataset
        self.uniqueItems = self.wlitems.distinct()

    def fit(self):
        supportRdd = self.wlitems.map(lambda item: (item, 1))
        supportRdd = supportRdd.reduceByKey(sumOparator)
        supports = supportRdd.map(lambda item: item[1])

        # Define minimum support value
        if self.minSupport == 'auto':
            minSupport = supports.min()
        else:
            minSupport = self.minSupport

        # If minimum support is 1 then replace it with 2
        minSupport = 2 if minSupport < 2 else minSupport

        # Filter first supportRdd with minimum support
        supportRdd = supportRdd.filter(lambda item: item[1] >= minSupport)

        # Create base RDD with will be updated every iteration
        baseRdd = supportRdd.map(lambda item: ([item[0]], item[1]))

        supportRdd = supportRdd.map(lambda item: item[0])

        c = 2

        while not supportRdd.isEmpty():
            combined = supportRdd.cartesian(self.uniqueItems)
            combined = combined.map(lambda item: removeReplica(item))

            combined = combined.filter(lambda item: len(item) == c)
            combined = combined.distinct()

            combined_2 = combined.cartesian(self.lblitems)
            combined_2 = combined_2.filter(lambda item: all(x in item[1] for x in item[0]))

            combined_2 = combined_2.map(lambda item: item[0])
            combined_2 = combined_2.map(lambda item: (item, 1))
            combined_2 = combined_2.reduceByKey(sumOparator)
            combined_2 = combined_2.filter(lambda item: item[1] >= minSupport)

            baseRdd = baseRdd.union(combined_2)

            combined_2 = combined_2.map(lambda item: item[0])
            supportRdd = combined_2
            c = c + 1

        sets = baseRdd.cartesian(baseRdd)
        filtered = sets.filter(lambda item: filterForConf(item))
        confidences = filtered.map(lambda item: calculateConfidence(item))
        self.confidences = confidences

        return confidences

    def predict(self, set, confidence):

        if not isinstance(set, list):
            raise ValueError('For prediction "set" argument should be a list')

        _confidences = self.confidences
        _filterForPredict = self._filterForPredict

        filtered = _confidences.filter(lambda item: _filterForPredict(item, set, confidence))

        return filtered

    @staticmethod
    def _filterForPredict(item, set, confidence):
        it = item[0]
        it.sort()
        set.sort()
        if it == set and item[2] >= confidence:
            return item
        else:
            pass
