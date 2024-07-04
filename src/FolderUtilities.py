import os
import logger

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





