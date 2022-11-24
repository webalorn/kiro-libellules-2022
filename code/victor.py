import random

class Job:
    def __init__(self, job_id, seq, rel, due, w, tasks):
        self.id = job_id
        self.seq = [tasks[k] for k in seq]
        self.rel = rel
        self.due = due
        self.w = w

        self.next_task = 0
        self.remt = 0

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


class Task:
    def __init__(self, task_id, time, job_id, using):
        self.id = task_id
        self.time = time
        self.job_id = job_id
        self.using = using

class State:
    def __init__(self, data):
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
            wps = job.get_workpairs().copy()
            random.shuffle(wps)
            for wp in job.get_workpairs():
                if self.is_workpair_free(wp):
                    self.assign_work(job.next_id(), wp, job.next_time())
                    job.work(self.time)
                    break

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
            print("left:", self.nb_left())
            print(self.remt_machines)
            print(self.remt_operators)
            self.very_basic_plan()
            self.step()
        return self.output()

def process_very_basic(data):
    x = State(data)
    return x.very_basic_sol()
