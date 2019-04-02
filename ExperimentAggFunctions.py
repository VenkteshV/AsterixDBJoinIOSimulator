class ExperimentAggFunctions:
    def __init__(self,runs):
        self.runs = runs

    @staticmethod
    def groupBy(runs, groupBy_fn):
        groups = {}
        for r in runs:
            g = groupBy_fn(r.config)
            groups[g] = groups.get(g, []) + [r]
        return groups

    def select(groups, select_fn):
        result = {}
        for g, runs in groups.items():
            result[g] = [select_fn(r.config) for r in runs]
        return result



