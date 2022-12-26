# Adapted from https://github.com/leonthamhy/RuleGrowth/blob/master/RuleGrowth.py
import time
import warnings
from math import ceil

from utils import load_data


class RuleGrowth:
    def __init__(self):
        self.database = None
        self.rules = []  # (antecedent, consequent, support, confidence)
        self.minsup_relative = None
        self.minconf = None
        self.itemlocmap = {}  # {Item: {seqId: [first, last]}}
        self.above_threshold = []
        self.max_antecedent = None
        self.max_consequent = None
        self.bin = None  # not in use
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
        if bin_interval:
            warnings.warn("bin_interval is not in use and will be ignored.", UserWarning)
        else:
            self.binning = self.zero_binning

        # 1. Initialize RuleGrowth members
        self.database = database
        self.rules = []
        self.minsup_relative = ceil(minsup * len(database))
        self.minconf = minconf
        self.itemlocmap = {}
        self.above_threshold = []
        self.max_antecedent = max_antecedent
        self.max_consequent = max_consequent
        self.bin = bin_interval
        # 1. END

        self.get_min_support_items()
        self.generate_rules()
        self.sort_rules()
        self.finish_fitting()
        # self.clear_memory()
        return self.rules

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
                    if occurrences_i[seqId][0] < occurrences_j[seqId][1]:
                        all_sequences_i_j.add(seqId)
                    if occurrences_j[seqId][0] < occurrences_i[seqId][1]:
                        all_sequences_j_i.add(seqId)

                # Check I => J
                if len(all_sequences_i_j) >= self.minsup_relative:
                    confidence_i_j = len(all_sequences_i_j) / len(occurrences_i)
                    antecedent_set = {item_i}
                    consequent_set = {item_j}

                    # Add those with valid support and confidence
                    if confidence_i_j >= self.minconf:
                        self.rules.append((
                            antecedent_set, consequent_set, len(all_sequences_i_j) / len(self.database), confidence_i_j
                        ))

                    # Expand left if possible
                    if not self.max_antecedent or len(antecedent_set) < self.max_antecedent:
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
                    if confidence_j_i >= self.minconf:
                        self.rules.append((
                            antecedent_set, consequent_set, len(all_sequences_j_i) / len(self.database), confidence_j_i
                        ))

                    # TODO: Make if inside expand left or right
                    if not self.max_antecedent or len(antecedent_set) < self.max_consequent:
                        self.expand_left(antecedent_set, consequent_set, all_sequences_j, all_sequences_j_i,
                                         occurrences_i)

                    if not self.max_consequent or len(consequent_set) < self.max_consequent:
                        self.expand_right(antecedent_set, consequent_set, all_sequences_j, all_sequences_i,
                                          all_sequences_j_i, occurrences_j, occurrences_i)

    def expand_left(self, antecedent_set, consequent_set, all_seq_i, all_seq_i_j, occurrences_j):
        """
        This function builds on an existing rule by adding an item to the antecedent set.

        :param antecedent_set:
        :param consequent_set:
        :param all_seq_i:
        :param all_seq_i_j:
        :param occurrences_j:
        :return:
        """
        possible_c = dict()
        # total number of possible sequences
        sequences_left = len(all_seq_i_j)

        for seqId in all_seq_i_j:
            sequence = self.database[seqId]
            first_j, last_j = occurrences_j[seqId]

            # For each itemset before the itemset containing the last occurrence of J in the sequence
            for itemset_id in range(last_j):
                itemset = sequence[itemset_id]

                for item in itemset:
                    # TODO: make method
                    if any([i >= item for i in
                            antecedent_set]) or item in consequent_set or item not in self.above_threshold:
                        # Ensure that the item is not already present in either
                        # antecedent or consequent set
                        # To prevent repeated rules, only item greater in value
                        # than all items inside the consequent set will be considered
                        continue
                    if item not in possible_c:
                        # items that meet support requirements are added
                        if sequences_left >= self.minsup_relative:
                            possible_c[item] = {seqId}
                    elif len(possible_c[item]) + sequences_left < self.minsup_relative:
                        # It is no longer possible to meet support requirements
                        del possible_c[item]
                    else:
                        possible_c[item].add(seqId)

            # Decrease max possible sequence left
            sequences_left -= 1

        # Loop through possible_c to generate valid rules
        for item_c, seqIds in possible_c.items():
            # Check if support is met
            if len(seqIds) >= self.minsup_relative:
                all_seq_i_c = set.intersection(all_seq_i, self.itemlocmap[item_c].keys())

                # Confidence of I C => J
                confidence_ic_j = len(seqIds) / len(all_seq_i_c)

                # new antecedent set
                items_i_c = antecedent_set.copy()
                items_i_c.add(item_c)

                # Add rule
                if confidence_ic_j >= self.minconf:
                    self.rules.append((
                        items_i_c, consequent_set, len(seqIds) / len(self.database), confidence_ic_j
                    ))

                if not self.max_antecedent or len(items_i_c) < self.max_antecedent:
                    self.expand_left(items_i_c, consequent_set, all_seq_i_c, seqIds, occurrences_j)

    def expand_right(self, antecedent_set, consequent_set, all_seq_i, all_seq_j, all_seq_i_j, occurrences_i,
                     occurrences_j):
        possible_c = dict()
        sequences_left = len(all_seq_i_j)

        for seqId in all_seq_i_j:
            sequence = self.database[seqId]
            first_i, last_i = occurrences_i[seqId]

            # For each itemset id after the itemset containing the first occurrence of I in the sequence
            for itemset_id in range(first_i + 1, len(sequence)):
                itemset = sequence[itemset_id]

                for item in itemset:
                    if any([i >= item for i in
                            consequent_set]) or item in antecedent_set or item not in self.above_threshold:
                        # Ensure that the item is not already present in either
                        # antecedent or consequent set
                        # To prevent repeated rules, only item greater in value
                        # than all items inside the consequent set will be considered
                        continue
                    if item not in possible_c:
                        if sequences_left >= self.minsup_relative:
                            possible_c[item] = {seqId}
                    elif len(possible_c[item]) + sequences_left < self.minsup_relative:
                        del possible_c[item]
                    else:
                        possible_c[item].add(seqId)
            sequences_left -= 1

        for item_c, seqIds in possible_c.items():
            if len(seqIds) >= self.minsup_relative:
                all_seq_j_c = set()
                occurrences_j_c = dict()

                for seqId_j in all_seq_j:
                    occurrences_c = self.itemlocmap[item_c].get(seqId_j, False)
                    if not occurrences_c:
                        continue
                    all_seq_j_c.add(seqId_j)
                    first_j, last_j = occurrences_j[seqId_j]
                    if occurrences_c[1] < last_j:
                        occurrences_j_c[seqId_j] = occurrences_c
                    else:
                        occurrences_j_c[seqId_j] = (first_j, last_j)

                confidence_i_jc = len(seqIds) / len(all_seq_i)

                items_j_c = consequent_set.copy()
                items_j_c.add(item_c)

                if confidence_i_jc >= self.minconf:
                    # TODO: Cache db len
                    self.rules.append((
                        antecedent_set, items_j_c, len(seqIds) / len(self.database), confidence_i_jc
                    ))

                if not self.max_antecedent or len(antecedent_set) < self.max_antecedent:
                    self.expand_left(antecedent_set, items_j_c, all_seq_i, seqIds, occurrences_j_c)

                if not self.max_consequent or len(items_j_c) < self.max_consequent:
                    self.expand_right(antecedent_set, items_j_c, all_seq_i, all_seq_j_c, seqIds, occurrences_i,
                                      occurrences_j_c)

    def binning(self, x):
        """
        self.bin must not be 0.
        Bin confidence by rounding down to nearest 0.05.
        binning(0.44) => 0.4
        binning(0.45) => 0.45
        binning(0.51) => 0.5
        :param x:
        :return:
        """
        intervals = 1 / self.bin
        return int(x * intervals) / intervals

    @staticmethod
    def zero_binning(x):
        return x

    def sort_rules(self):
        """
        Sorts the rules in the followinf order of importance:
        1. Binned confidence
        2. Length of antecedent
        3. Confidence
        :return:
        """
        # TODO: uncomment
        # Sort by length of antecedent
        # self.rules = sorted(self.rules, key=lambda x: len(x[0]), reverse=True)
        # Sort by binned confidence
        self.rules = sorted(self.rules, key=lambda x: self.binning(x[3]), reverse=True)

    def clear_memory(self):
        self.database = None
        self.itemlocmap = {}
        self.above_threshold = []

    def finish_fitting(self):
        unique_consequent_items = 0
        items = [rule[1] for rule in self.rules]
        if len(items):
            unique_consequent_items = len(set.union(*items))
        print(f"RuleGrowth fitted with {len(self.rules)} rules and {unique_consequent_items} unique consequent items.")

        self.fitted = True


if __name__ == '__main__':
    rule_growth = RuleGrowth()

    dataset = load_data()

    start = time.time()
    rule_growth.fit(dataset, 0.01, 0.01)
    print("Time: ", time.time() - start)

    for rule in rule_growth.rules:
        print(rule)
