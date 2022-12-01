# Databricks notebook source
# dbutils.notebook.run("./configs/config",0, {"Source of Data": "Ergast"}) # example to pass parameters

# COMMAND ----------

notebook_path_list=['./csv/Results',
                    './csv/Circuits', 
                    './csv/LapTimes', 
                    './csv/Races', 
                    './Json/Constructors',
                    './Json/Drivers', 
                    './Json/Pitstops', 
                    './Json/Qualifying']

# COMMAND ----------

def ingest_file(notebook_path):
    file = notebook_path.split('/')[-1]
    print(f'{file} is running...')
    dbutils.notebook.run(notebook_path,0)
    print(f'..........{file} notebook completed')

# COMMAND ----------

# for notebook_path in notebook_path_list:
#     ingest_file(notebook_path) # without multi threading it takes 6 min

# COMMAND ----------

from queue import Queue

q=Queue()

for notebook_path in notebook_path_list:
    q.put(notebook_path)

# COMMAND ----------

def run_tasks(function, q):
    while not q.empty():
        val = q.get()
        ingest_file(val)
        q.task_done()

# COMMAND ----------

from threading import Thread
workers = 4
for i in range(workers):
    t = Thread(target=run_tasks, args=(notebook_path, q))
    t.daemon = True
    t.start()
q.join()
