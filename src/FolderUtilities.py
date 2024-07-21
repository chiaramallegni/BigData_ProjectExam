import os


class FolderUtilities:
    def __init__(self, parent_dir, folder, mode):
        self.parent_dir=parent_dir
        self.folder=folder
        self.mode=mode


# create folder if not exist
def create_folder(parent_dir, folder, mode):
    directory = os.path.join(parent_dir,folder)
    if os.path.exists(directory):
        print("Directory '%s' exists" % directory)
    if not os.path.exists(directory):
        os.mkdir(directory, mode)
        print("Directory '%s' created" % directory)
    return directory


def getShowStringForLog(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))


