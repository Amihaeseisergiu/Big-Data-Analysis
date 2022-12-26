# Adapted from https://github.com/leonthamhy/RuleGrowth/blob/master/RuleGrowth.py

import warnings
from math import ceil
from utils import load_data


class RuleGrowth:
    def __init__(self):
        self.database = None
        self.rules = []  # (antecedent, consequent, support, confidence)
        self.minsup = None
        self.minsup_relative = None
        self.minconf = None
        self.itemlocmap = {}  # {Item: {seqId: [first, last]}}
        self.above_threshold = []
        self.max_antecedent = None
        self.max_consequent = None
        # self.bin = None  # not in use
        self.fitted = False  # TODO: is this really useful?

    def fit(self, database, minsup, minconf, max_antecedent=0, max_consequent=0, bin_interval=0):
        """
        Generate sequential rules based on RuleGrowth

        :param database: history of purchases
        :param minsup: minimum fraction of I => J occurances in all sequences
        :param minconf: minimum fraction of I => J occurances in sequences with I
        :param max_antecedent: Maximum number of items in antecedent set (default: 0)
        :param max_consequent: Maximum number of items in the consequent set (default: 0)
        :param bin_interval: interval to bin rules for sorting (NOT IN USE)
        :return:
        """
        assert isinstance(minsup, float) and 0 < minsup <= 1, "Support has to be in range (0, 1]"
        assert isinstance(minconf, float) and 0 < minconf <= 1, "Support has to be in range (0, 1]"
        assert isinstance(max_antecedent, int) and max_antecedent >= 0, "Max antecedent has to be a positive integer"
        assert isinstance(max_consequent, int) and max_consequent >= 0, "Max consequent has to be a positive integer"
        assert isinstance(bin_interval, (float, int)) and bin_interval == 0 or int(
            1 / bin_interval) == 1 / bin_interval, "Yeah"
        if bin:
            warnings.warn("bin_interval is not in use and will be ignored.", UserWarning)

        # 1. Initialize RuleGrowth members
        self.database = database
        self.rules = []
        self.minsup = minsup
        self.minsup_relative = ceil(minsup * len(database))
        self.minconf = minconf
        self.itemlocmap = {}
        self.above_threshold = []
        self.max_antecedent = max_antecedent
        self.max_consequent = max_consequent
        # 1. END

        self.get_min_support_items()
        self.generate_rules()

    def get_min_support_items(self):
        """
        Function that generates itemlocmap and records items that are above support threshold.
        Removes items below threshold of support within database
        :return:
        """
        # For each sequence
        for seqId, sequence in enumerate(self.database):
            # For each itemset in sequence
            for idx, itemset in enumerate(sequence):
                # For each item in itemset  # TODO: remove if useless
                for item in itemset:
                    if item not in self.itemlocmap:
                        self.itemlocmap[item] = {seqId: [idx, idx]}
                    elif seqId not in self.itemlocmap[item]:
                        self.itemlocmap[item][seqId] = [idx, idx]
                    else:
                        self.itemlocmap[item][seqId][1] = idx

        below_threshold = []
        for item, value in self.itemlocmap.items():
            if len(value) < self.minsup_relative:
                below_threshold.append(item)
            else:
                self.above_threshold.append(item)

        # Code for removing below_threshold items, TODO

    def generate_rules(self):
        """
        This function first generates valid rules of size 2.
        Then it will recursively expand the rules using expand left and expand right
        :return:
        """
        for i in range(len(self.above_threshold)):
            for j in range(i + 1, len(self.above_threshold)):
                # Items
                item_i = self.above_threshold[i]
                item_j = self.above_threshold[j]
                occurrences_i = self.itemlocmap[item_i]
                occurrences_j = self.itemlocmap[item_j]
                all_sequences_i = set(occurrences_i.keys())
                all_sequences_j = set(occurrences_j.keys())

                all_sequences_i_j = set()
                all_sequences_j_i = set()

                all_sequences_both = set.intersection(all_sequences_i, all_sequences_j)

                # Sequences that have I => J or J => I
                for seqId in all_sequences_both:
                    if occurrences_i[seqId][0] < occurrences_i[seqId][1]:
                        all_sequences_i_j.add(seqId)
                    if occurrences_i[seqId][0] < occurrences_i[seqId][1]:
                        all_sequences_j_i.add(seqId)

                # Check I => J
                if len(all_sequences_i_j) >= self.minsup_relative:
                    confidence_i_j = len(all_sequences_i_j) / len(occurrences_i)
                    antecedent_set = {item_i}
                    consequent_set = {item_j}

                    # Add those with valid support and confidence
                    if confidence_i_j >= self.minconf:
                        to_add = (
                            antecedent_set, consequent_set, len(all_sequences_i_j) / len(self.database), confidence_i_j)
                        self.rules.append(to_add)

                    # Expand left if possible
                    if not self.max_antecedent or len(antecedent_set) <= self.max_antecedent:
                        # TODO: indices
                        self.expand_left(antecedent_set, consequent_set, all_sequences_i,
                                         all_sequences_i_j, occurrences_j)

                    # Expand right if possible
                    if not self.max_consequent or len(consequent_set) < self.max_consequent:
                        self.expand_right(antecedent_set, consequent_set, all_sequences_i, all_sequences_j,
                                          all_sequences_i_j, occurrences_i, occurrences_j)

                # TODO: Make method
                # Check J => I
                if len(all_sequences_j_i) >= self.minsup_relative:
                    confidence_j_i = len(all_sequences_j_i) / len(occurrences_j)
                    antecedent_set = {item_j}
                    consequent_set = {item_i}

                    # Add those with valid support and confidence
                    if confidence_j_i >= self.minsup_relative:
                        to_add = (
                            antecedent_set, consequent_set, len(all_sequences_j_i) / len(self.database), confidence_j_i)
                        self.rules.append(to_add)

                    # TODO: Make if inside expand left or right
                    if not self.max_antecedent or len(antecedent_set) < self.max_consequent:
                        self.expand_left(antecedent_set, consequent_set, all_sequences_j, all_sequences_j_i,
                                         occurrences_i)

                    if not self.max_consequent or len(consequent_set) < self.max_consequent:
                        self.expand_right(antecedent_set, consequent_set, all_sequences_j, all_sequences_i,
                                          all_sequences_j_i, occurrences_j, occurrences_i)

    def expand_left(self, antecedent_set, consequent_set, all_seq_i, all_seq_i_j, occurrences_j):
        # TODO: implement
        pass

    def expand_right(self, antecedent_set, consequent_set, all_seq_i, all_seq_j, all_seq_i_j, occurrences_i,
                     occurrences_j):
        # TODO: implement
        pass


if __name__ == '__main__':
    rule_growth = RuleGrowth()

    dataset = load_data()

    rule_growth.fit(dataset, 0.1, 0.1)
