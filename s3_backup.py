"""
Backup server contents to s3 in encrypted form
"""
from multiprocessing import Process
import time

repositories = None

def init_module(repos):
    global repositories
    repositories = repos

    p = Process(target=backup_process, args=())
    p.start()

def backup_process():
    global repositories

    while True:
        print 'S3 backup process'
        time.sleep(10)


