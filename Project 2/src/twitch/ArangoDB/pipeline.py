from database_init import initialization
import subprocess
import time
import psutil
import os

def get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss

def track(func):
    def wrapper(*args, **kwargs):
        mem_before = get_process_memory()/1024/1024
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start
        mem_after = get_process_memory()/1024/1024
        print("{}: memory before: {:,} MB, after: {:,} MB, consumed: {:,} MB; exec time: {}".format(
            func.__name__,
            mem_before, mem_after, (mem_after - mem_before),
            elapsed_time))
        return result
    return wrapper

def run(cmd):
    completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)
    return completed

def arangosh_detection_task():
    detection_command  = "arangosh --server.endpoint tcp://localhost:8000 --server.database Twitch --javascript.execute detection_task.js"
    detection_info = run(detection_command)
    if detection_info.returncode != 0:
        print("An error occured: %s", detection_info.stderr)
    else:
        print("Detection command executed successfully!")
    
    print("-------------------------")

@track
def databasePipeline():
    initialization()
    arangosh_detection_task()
    
if __name__ == '__main__':
    databasePipeline()  