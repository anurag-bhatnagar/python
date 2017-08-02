#!/usr/bin/python

import os
import re
import time
import sys
import threading
import platform
from random import choice
import Queue
import collections
import tempfile
import shutil

########################################################################
class FileGen(threading.Thread):
    """Threaded File Generator"""
 
    #----------------------------------------------------------------------
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
 
    #----------------------------------------------------------------------
    def run(self):
        while True:
            # get the filename from queue
            item = self.queue.get()
            if item is None:
              break
 
            # creates the file
            if perfFlag == 'Y':
                create_file_performance(item)
            else:
                create_file_regular(item)                            
 
            # send a signal to the queue that the job is done
            self.queue.task_done() 

#---------------------------------CREATE DIRECTORIES-------------------------------------
def dir_create():
    share=r"" + str(unc) if platform.system() == 'Windows' else str(unc)
    list = []
    for i in range(int(width)):
        path=share + "\\dir" + str(i) + "\\" if platform.system() == 'Windows' else share + "//dir" + str(i) + "/"
        list.append(path)
        if depth != 0:
            for j in range(int(depth)):
                path=path + "\\dir" + str(j) + "\\" if platform.system() == 'Windows' else share + "//dir" + str(i) + "/"
                list.append(path)
                if not os.path.exists(path):
                    os.makedirs(path)
                os.chdir(share)
        else:
            if not os.path.exists(path):
                os.makedirs(path)
            os.chdir(share)
    return list;

#---------------------------------CREATE TEMP CACHED FILES-------------------------------------
def cached_files():
    list = []
    for i in range(int(1073741824/int(choice(sizeList)))):
        f = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        f.write(os.urandom(int(choice(sizeList))))
        file_name = f.name
        list.append(file_name)
        f.close()
    return list;

#---------------------------------CREATE FILES FOR PERFORMANCE TESTING (10Mbps)-------------------------------------
def create_file_performance(filename):
    dirpath = choice(dirList)
    dirpath = dirpath + '\\' + filename if platform.system() == 'Windows' else dirpath + filename
    filesize = int(choice(sizeList))
    todo = filesize    
    with open(choice(cachedFileList), 'rb') as fsrc:
        with open(dirpath, 'wb') as fdst:
            shutil.copyfileobj(fsrc, fdst, 10485760 if filesize >= 10485760 else filesize )

#---------------------------------CREATE FILES FOR REGULAR TESTING (10Mbps)-------------------------------------
def create_file_regular(filename):
    dirpath = choice(dirList)
    filesize = int(choice(sizeList))
    os.chdir(dirpath)
    file = open(filename, 'w')
    todo = filesize
    while todo > 0:
        file.write(os.urandom(min(todo, 10485760)))
        todo = filesize - file.tell()
    file.close()
    os.chdir(unc)

#---------------------------------MAIN-------------------------------------
def main():
    # Step 1: Define queue length
    queue = Queue.Queue(1024)

    # Step 2: Spawn a pool of threads, and pass them queue instance
    filelist = []
    for i in range(threads):
        p = FileGen(queue)
        filelist.append(p)
        p.setDaemon(True)
        p.start()

    # Step 3: Now load jobs into the queue
    for i in range(int(totalFiles)):
        name = 'file' + str(i) + '.' + choice(extList)
        queue.put(name)    

    for i in range(threads):
        queue.put(None)    

    #Step 4: Wait on the queue until everything has been processed 
    for item in filelist:
        item.join()

    print "End Time   : " + time.ctime()
    raw_input('\nPress Enter key to Exit ...')

    

#---------------------------------INIT-------------------------------------
if __name__ == "__main__":
    
    # User Input
    print('\n' + '### FILEGEN SCRIPT ###' + '\n' + '\n')
    unc =           raw_input("1.) Enter UNC Path             [\\\srv\\fs]: ") if platform.system() == 'Windows' else raw_input("1.) Enter Path [/path]: ")
    totalFiles =    raw_input("2.) Total Files to be created        [10]: ")
    size =          raw_input("3.) Comma separated File Size      [10KB]: ")
    width =         raw_input("4.) Width of directory structure      [1]: ")
    depth =         raw_input("5.) Depth of directory structure      [0]: ")
    threads =       raw_input("6.) No. of Threads to be spawnned    [10]: ")
    extensions =    raw_input("7.) Comma separated File Extensions [txt]: ")
    perfFlag =      raw_input("8.) Create Performance Data Flag      [N]: ")

    print "\nStart Time : " + time.ctime()
    sizeList = size.split(',')
    extList = extensions.split(',')
    
    # Set DEFAULT Values, in case of no user input
    if not totalFiles:  totalFiles=10
    if not size:        sizeList=['10KB']
    if not width:       width=1
    if not depth:       depth=0
    if not threads:     threads=10
    if not extensions:  extList=['txt']
    if not perfFlag:    perfFlag='N'
    totalFiles = int(totalFiles)
    threads = int(threads)    

    # Convert Size to equivalent bytes
    for i in range(len(sizeList)):
        ss = re.match(r"(\d+)((B)|(KB)|(MB)|(GB)|(TB))",sizeList[i])
        #print ss.group(1),'test',ss.group(2),ss.lastindex,len(ss)
        if ss.group(2) == 'B':
            sizeList[i] = int(ss.group(1))
        if ss.group(2) == 'KB':
            sizeList[i] = int(ss.group(1))*1024
        if ss.group(2) == 'MB':
            sizeList[i] = int(ss.group(1))*1024*1024
        if ss.group(2) == 'GB':
            sizeList[i] = int(ss.group(1))*1024*1024*1024
        if ss.group(2) == 'TB':
            sizeList[i] = int(ss.group(1))*1024 *1024*1024*1024

    # First create complete directory structure
    dirList = dir_create()    

    # Create temp cached files for faster file generation
    if perfFlag == 'Y':
        cachedFileList = cached_files()    
   
    main()

# End of File
