import os
from graphframes import GraphFrame


class Utilities:
    def __init__(self):
        i = 1

    def create_folder(self, parent_dir, folder, mode):
        directory = os.path.join(parent_dir,folder)
        if not os.path.exists(directory):
            os.makedirs(directory, mode=mode)
            print(f"Directory '{directory}' created")
        else:
            print(f"Directory '{directory}' exists")
        return directory
    @staticmethod
    def getShowStringForLog(df, n=20, truncate=True, vertical=False):
        if isinstance(truncate, bool) and truncate:
            return(df._jdf.showString(n, 20, vertical))
        else:
            return(df._jdf.showString(n, int(truncate), vertical))

    def gen_graph_from_edgeList (slef, e):
        v = e \
            .select('src') \
            .union(e.select('dst')) \
            .distinct() \
            .withColumnRenamed('src', 'id')
        return GraphFrame(v,e)