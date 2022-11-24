import random
import math
from copy import deepcopy
from util import *

LAST_JOB_PENALITY = [0] * 200

def add_penality(in_data, out_data):
    score = 0
    penalities = [0] * in_data['nb_jobs']
    for job_id in range(in_data['nb_jobs']):
        last_task = in_data['jobs']['sequence'][job_id][-1]
        end_time = out_data['task_start'][last_task] + in_data['tasks']['time'][last_task]
        max_time = in_data['jobs']['due'][job_id]
        w = in_data['jobs']['weight'][job_id]
        score = w * end_time
        if end_time > max_time:
            score += w * UNIT_PENALTY
            score += w * TARDINESS_COST * (end_time - max_time)

        
        penalities[job_id] = score
    
    maxi = max(penalities)
    penalities = [math.log(p) for p in penalities]
    # penalities = [p/maxi for p in penalities]
    for i in range(len(penalities)):
        LAST_JOB_PENALITY[i] += penalities[i]
        

class Job:
    def __init__(self, job_id=None, seq=None, rel=None, due=None, w=None, tasks=None):
        if job_id is None: return
        self.id = job_id
        self.seq = [tasks[k] for k in seq]
        self.rel = rel
        self.due = due
        self.w = w

        self.next_task = 0
        self.remt = 0

    def copy(self):
        ret = Job()
        ret.load(self)

        return ret

    def load(self, other):
        self.id =  other.id
        self.seq = other.seq
        self.rel = other.rel
        self.due = other.due
        self.w =   other.w
        self.next_task = other.next_task
        self.remt = other.remt

    def finished(self):
        return self.next_task == len(self.seq)

    def nb_left(self):
        return len(self.seq) - self.next_task

    def is_released(self, time):
        return self.rel <= time

    def can_work(self, time):
        return self.is_released(time) and not self.finished()

    def busy(self):
        return self.remt != 0

    def sleeping(self, time):
        return self.can_work(time) and not self.busy()

    def work(self, time):
        assert not self.busy() and self.can_work(time)
        self.remt = self.next_time()
        self.next_task += 1

    def next_id(self):
        return self.seq[self.next_task].id

    def next_time(self):
        return self.seq[self.next_task].time

    def step(self):
        if self.busy():
            self.remt -= 1

    def get_workpairs(self):
        assert not self.finished()
        return self.seq[self.next_task].using

    def get_bonus_time(self, time):
        if self.finished(): return 0
        rem_work = sum([t.time for t in self.seq[self.next_task:]]) + self.remt
        rem_time = self.due - time
        return rem_time - rem_work

class Task:
    def __init__(self, task_id, time, job_id, using):
        self.id = task_id
        self.time = time
        self.job_id = job_id
        self.using = using

class State:
    def __init__(self, data=None):
        if data is None: return
        self.data = data
        self.nb_jobs = data["nb_jobs"]
        self.nb_tasks = data["nb_tasks"]
        self.nb_machines = data["nb_machines"]
        self.nb_operators = data["nb_operators"]

        self.tasks = [
            Task(i, t, j, u)
            for (i, (t, j, u)) in enumerate(zip(data["tasks"]["time"], data["tasks"]["job_id"], data["tasks"]["using"]))
        ]

        self.jobs = [
            Job(i, seq, rel, due, w, self.tasks)
            for (i, (seq, rel, due, w)) in enumerate(zip(data["jobs"]["sequence"], data["jobs"]["release"], data["jobs"]["due"], data["jobs"]["weight"]))
        ]

        self.time = 0
        self.remt_machines = [0 for k in range(self.nb_machines)]
        self.remt_operators = [0 for k in range(self.nb_operators)]

        self.trace = []

    def copy(self):
        ret = State()
        ret.load(self)

        return ret

    def load(self, other):
        self.data =         other.data
        self.nb_jobs =      other.nb_jobs
        self.nb_tasks =     other.nb_tasks
        self.nb_machines =  other.nb_machines
        self.nb_operators = other.nb_operators
        self.jobs = [j.copy() for j in other.jobs]
        self.time = other.time
        self.remt_machines = deepcopy(other.remt_machines)
        self.remt_operators = deepcopy(other.remt_operators)
        self.trace = deepcopy(other.trace)

    def is_workpair_free(self, pair):
        return self.remt_machines[pair[0]] == 0 and self.remt_operators[pair[1]] == 0

    def assign_work(self, task_id, pair, dur):
        assert self.is_workpair_free(pair)
        self.trace.append((task_id, pair, self.time))
        self.remt_machines[pair[0]] = dur
        self.remt_operators[pair[1]] = dur

    def step(self):
        for job in self.jobs:
            job.step()
        self.time += 1
        for k in range(self.nb_machines):
            self.remt_machines[k] = max(0, self.remt_machines[k] - 1)
        for k in range(self.nb_operators):
            self.remt_operators[k] = max(0, self.remt_operators[k] - 1)

    def very_basic_plan(self):
        random.shuffle(self.jobs)
        for job in self.jobs:
            if not job.sleeping(self.time): continue
            for wp in job.get_workpairs():
                if self.is_workpair_free(wp):
                    self.assign_work(job.next_id(), wp, job.next_time())
                    job.work(self.time)
                    break

    def cautious_plan(self):
        def job_rank(job):
            t = job.get_bonus_time(self.time)
            if t < 0:
                # t = (t*2+6) * job.w ** 0.7
                t = t * job.w
                # t = -math.log(1-t)
            else:
                t = t * job.w
                # t = math.log(1+t)
            t = t + t * random.random() * 0.1
            p = LAST_JOB_PENALITY[job.id]
            t = t * (1+p)
            return t
        self.jobs.sort(key=job_rank)
        for job in self.jobs:
            if not job.sleeping(self.time): continue
            wps = job.get_workpairs().copy()
            random.shuffle(wps)
            for wp in job.get_workpairs():
                if self.is_workpair_free(wp):
                    self.assign_work(job.next_id(), wp, job.next_time())
                    job.work(self.time)
                    break

    def cautious_force_plan(self):
        # self.jobs.sort(key=(lambda x: x.get_bonus_time(self.time)))
        def job_rank(job):
            t = job.get_bonus_time(self.time)
            t = t * job.w
            t = t + t * random.random() * 0.2
            return t
        self.jobs.sort(key=job_rank)

        best_nb = -1
        best_state = None
        for k in range(5):
            tmp = self.copy()
            nb = 0
            for job in tmp.jobs:
                if not job.sleeping(tmp.time): continue
                wps = job.get_workpairs().copy()
                random.shuffle(wps)
                for wp in job.get_workpairs():
                    if tmp.is_workpair_free(wp):
                        nb += 1
                        tmp.assign_work(job.next_id(), wp, job.next_time())
                        job.work(tmp.time)
                        break
            if nb > best_nb:
                best_nb = nb
                best_state = tmp
        self.load(best_state)

    def finished(self):
        return all([job.finished() for job in self.jobs])

    def nb_left(self):
        return sum([job.nb_left() for job in self.jobs])

    def output(self):
        task_to = [None for k in range(self.nb_tasks)]
        task_start = [None for k in range(self.nb_tasks)]

        for tr in self.trace:
            task_to[tr[0]] = tr[1]
            task_start[tr[0]] = tr[2]

        return {"task_to": task_to, "task_start": task_start}

    def very_basic_sol(self):
        while not self.finished():
            self.very_basic_plan()
            self.step()
        return self.output()

    def cautious_sol(self):
        while not self.finished():
            self.cautious_plan()
            self.step()
        return self.output()

    def cautious_force_sol(self):
        while not self.finished():
            self.cautious_force_plan()
            self.step()
        return self.output()

def process_very_basic(data):
    x = State(data)
    return x.very_basic_sol()

def process_cautious(data):
    x = State(data)
    return x.cautious_sol()

def process_cautious_force(data):
    x = State(data)
    return x.cautious_force_sol()

def process(data):
    return process_cautious(data)
    return process_cautious_force(data)
