from pathlib import Path
from collections import deque, namedtuple
from math import *
from random import randint, shuffle

import numpy as np

import json
import time

BEST_SOLS = {}
BEST_SOLS_DATA = {}
IN_DATA = {}
INPUT_NAMES = [e.name for e in Path('../inputs').iterdir() if e.name.endswith('.json')]

OUT_SUFFIX = '-out-1' # TODO : to have different solutions names

# ========== Constants ==========

UNIT_PENALTY = 6
TARDINESS_COST = 1

# ========== Functions for this specific problem ==========

def task_ids_sorted_by_time(times):
    task_ids = list(range(len(times)))
    task_ids.sort(key = lambda x : times[x])
    return task_ids


# ========== Compute vals on sols ==========

def generate_empty_solution(in_data):
    pass # TODO, if needed

# ========== Input / Output ==========

def preprocess_input(data):

    preprocess_data = dict()

    # keys nb_jobs, nb_tasks, nb_machines, nb_operators
    nb_jobs = data["parameters"]["size"]["nb_jobs"]
    nb_tasks = data["parameters"]["size"]["nb_tasks"]
    nb_machines = data["parameters"]["size"]["nb_machines"]
    nb_operators = data["parameters"]["size"]["nb_operators"]
    preprocess_data["nb_jobs"] = nb_jobs
    preprocess_data["nb_tasks"] = nb_tasks
    preprocess_data["nb_machines"] = nb_machines
    preprocess_data["nb_operators"] = nb_operators

    # key jobs
    jobs = dict()

    job_id = [0]*nb_tasks # for the key tasks

    sequence = [0]*nb_jobs
    release = [0]*nb_jobs
    due = [0]*nb_jobs
    weight = [0]*nb_jobs

    info_job = data["jobs"]
    for job_i in info_job:
        ind = job_i["job"] - 1
        s = job_i["sequence"][::]
        for elt in range(len(s)):
            s[elt] -= 1
            job_id[s[elt]] = ind
        sequence[ind] = s

        release[ind] = job_i["release_date"]
        
        due[ind] = job_i["due_date"]

        weight[ind] = job_i["weight"]

    jobs["sequence"] = sequence
    jobs["release"] = release
    jobs["due"] = due
    jobs["weight"] = weight

    preprocess_data["jobs"] = jobs

    # key tasks
    tasks = dict()

    time = [0]*nb_tasks
    using = [[] for _ in range(nb_tasks)]

    tasks_info = data["tasks"]
    for task_i in tasks_info:
        ind = task_i["task"] - 1

        time[ind] = task_i["processing_time"]

        possible_use = task_i["machines"]
        for couple in possible_use:
            op = couple["operators"][::]
            for elt in range(len(op)):
                op[elt] -= 1
                using[ind].append((couple["machine"]-1,op[elt]))
    
    tasks["time"] = time
    tasks["job_id"] = job_id
    tasks["using"] = using

    preprocess_data["tasks"] = tasks

    return preprocess_data

def read_input(name):
    p = Path('../inputs') / name
    with open(str(p), 'r') as f:
        data = json.load(f)
    return preprocess_input(data)

def read_all_inputs():
    for name in INPUT_NAMES:
        IN_DATA[name] = read_input(name)

def _out_with_suffix(name):
    return name[:-5] + OUT_SUFFIX + name[-5:]

def read_sol(name):
    p = Path('../sols') / _out_with_suffix(name)
    with open(str(p), 'r') as f:
        data = json.load(f)
    return data

def transform_to_output(data):
    assert False
    return [
        {"task": k+1, "start": t, "machine": m+1, "operator": o+1}
        for (k, ((m, o), t)) in enumerate(zip(data["task_to"], data["task_start"]))
    ]

def output_sol_force_overwrite(name, data):
    p = Path('../sols') / _out_with_suffix(name)
    with open(str(p), 'w') as f:
        json.dump(data, f)

def output_sol_if_better(name, data):
    """ Returns True if the solution is better than the last found solution in this program run,
        even solution already written in the JSON file is even better.
        Updates BEST_SOLS_DATA and BEST_SOLS """
    sol_val = eval_sol(IN_DATA[name], data, check=True)
    if name in BEST_SOLS and is_better_sol(sol_val, BEST_SOLS[name]):
        return False
    BEST_SOLS[name] = sol_val
    BEST_SOLS_DATA[name] = data

    cur_file_sol = None
    try:
        cur_file_sol = read_sol(name)
    except:
        pass
    if cur_file_sol is not None:
        old_val = eval_sol(IN_DATA[name], cur_file_sol)
        if not is_better_sol(old_val, sol_val):
            return True
    print(f"----> Found solution for {name} of value {sol_val}")
    output_sol_force_overwrite(name, data)
    return True

# ========== Evaluation ==========

def no_recouvrement(l):
    for i in range (len(l)-1):
        deb1,fin1 = l[i]
        deb2,fin2 = l[i+1]
        if deb2 < fin1:
            return False
    
    return True

def eval_sol(in_data, out_data, check=False):
    # Assert sol is good
    if check:
        # check if there is the correct number of items
        assert len(out_data['task_to']) == len(out_data['task_start'])
        assert len(out_data['task_to']) == in_data['nb_tasks']

        # check if an op can use this machine for the task and if the task begins after the previous one
        for job_id in range(in_data['nb_jobs']):
            rel = in_data['jobs']['release'][job_id]
            for task_id in in_data['jobs']['sequence'][job_id]:
                start_time = out_data['task_start'][task_id]
                end_time = start_time + in_data['tasks']['time'][task_id]
                # task_times.append((start_time, end_time))
                assert start_time >= rel, task_id
                rel = end_time
                assert out_data['task_to'][task_id] in in_data['tasks']['using'][task_id]
            
        # check if not 2 machines / operator used at the same time
        mach_used = [[] for _ in range(in_data['nb_machines'])]
        op_used = [[] for _ in range (in_data['nb_operators'])]

        for id_task in range(in_data['nb_tasks']):
            debut_time = out_data['task_start'][id_task]
            machine = out_data['task_to'][id_task][0]
            op = out_data['task_to'][id_task][1]
            end_time = debut_time+in_data['tasks']['time'][id_task]
            mach_used[machine].append((debut_time,end_time))
            op_used[op].append((debut_time,end_time))

        for i in range(in_data['nb_operators']):
            op_used[i] = sorted(op_used[i])
            assert no_recouvrement(op_used[i])
        
        for i in range(in_data['nb_machines']):
            mach_used[i] = sorted(mach_used[i])
            assert no_recouvrement(op_used[i])

        

    # Score
    score = 0
    for job_id in range(in_data['nb_jobs']):
        last_task = in_data['jobs']['sequence'][job_id][-1]
        end_time = out_data['task_start'][last_task] + in_data['tasks']['time'][last_task]
        max_time = in_data['jobs']['due'][job_id]
        w = in_data['jobs']['weight'][job_id]
        score += w * end_time
        if end_time > max_time:
            score += w * UNIT_PENALTY
            score += w * TARDINESS_COST * (end_time - max_time)

    return score

def is_better_sol(old_sol_value, new_sol_value):
    return new_sol_value < old_sol_value # TODO : Replace by < if the best value is the lower one
            

# ========== Utilities ==========

COLORS = {
    'PURPLE': '\033[95m',
    'BLIE': '\033[94m',
    'CYAN': '\033[96m',
    'GREEN': '\033[92m',
    'ORANGE': '\033[93m',
    'RED': '\033[91m',
    'END': '\033[0m',
    'BOLD': '\033[1m',
    'UNDERLINE': '\033[4m',
}

def _print_color(color, *args, **kwargs):
    print(f"{color}{args[0]}", *args[1:], '\033[0m', **kwargs)

def print_err(*args, **kwargs):
    _print_color("\033[91m", "[ERROR]", *args, **kwargs)

def print_info(*args, **kwargs):
    _print_color("\033[94;1m", "[INFO]", *args, **kwargs)

def print_warning(*args, **kwargs):
    _print_color("\033[93m", "[WARNING]", *args, **kwargs)

def print_ok(*args, **kwargs):
    _print_color("\033[92m", "[WARNING]", *args, **kwargs)

class Heap(): # Smaller number on top
    def __init__(self, l=[]):
        self.l = copy(l)
        if self.l: heapq.heapify(self.l)
    def push(self, el): return heapq.heappush(self.l, el)
    def top(self): return self.l[0]
    def pop(self): return heapq.heappop(self.l)
    def size(self): return len(self.l)
    def empty(): return self.l == []
