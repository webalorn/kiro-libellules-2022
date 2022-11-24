from util import *
from copy import copy, deepcopy
import random

# N_RANKED = 4
# MAX_DEPTH = 4
# PENALTY_NOT_ASSIGNED_TASK = 1

N_RANKED = 5
MAX_DEPTH = 4
PENALTY_NOT_ASSIGNED_TASK = 0.5

def scoring1(in_data, cur_time, assigned_times):
    score = 0
    for job_id in range(in_data['nb_jobs']):
        end_time = in_data['jobs']['release'][job_id]
        for task_id in in_data['jobs']['sequence'][job_id]:
            task_duration = in_data['tasks']['time'][job_id]
            if assigned_times[task_id] > -1:
                end_time = assigned_times[task_id] + task_duration
            else:
                end_time += task_duration + PENALTY_NOT_ASSIGNED_TASK

        max_time = in_data['jobs']['due'][job_id]
        w = in_data['jobs']['weight'][job_id]
        score += w * end_time
        if end_time > max_time:
            score += w * UNIT_PENALTY
            score += w * TARDINESS_COST * (end_time - max_time)
    return score

def select_ranked_pool(in_data, pool):
    nb_of_taskid = {}
    random.shuffle(pool)
    ranked_pool = []
    for task in pool:
        task_id, (machine_id, ope_id) = task
        if nb_of_taskid.get(task_id, 0) < 2:
            nb_of_taskid[task_id] = nb_of_taskid.get(task_id, 0)+1
            ranked_pool.append(task)

    def eval_rank(task):
        task_id, (machine_id, ope_id) = task
        job_id = in_data['tasks']['job_id'][task_id]
        w = in_data['jobs']['weight'][job_id]
        return w
    ranked_pool.sort(key=eval_rank)
    return ranked_pool[::-1][:N_RANKED]

def explore_sol(in_data, cur_time, realease_order, last_realeased, task_next, task_pool, assigned_times, predict_depth, machine_end, ope_end, prev_task):
    while last_realeased < len(realease_order) and in_data['jobs']['release'][realease_order[last_realeased]] <= cur_time:
        task_pool.append(in_data['jobs']['sequence'][realease_order[last_realeased]][0])
        last_realeased += 1
    
    ranked_pool = [] # TODO: Select if possible
    for task_id in task_pool:
        prev = prev_task[task_id]
        if prev != -1:
            prev_end = assigned_times[prev] + in_data['tasks']['time'][prev]
            if prev_end > cur_time:
                continue
        for machine_id, ope_id in in_data['tasks']['using'][task_id]:
            if machine_end[machine_id] <= cur_time and ope_end[ope_id] <= cur_time:
                ranked_pool.append((task_id, (machine_id, ope_id)))
    
    if predict_depth > 1:
        ranked_pool = select_ranked_pool(in_data, ranked_pool)
    #     # TODO: if not main, ranked task pool

    if predict_depth < MAX_DEPTH and len(ranked_pool):
        best_subtask_score, best_subtask = None, None

        for task_to_do, (machine_id, ope_id) in ranked_pool:
            assigned_times[task_to_do] = cur_time
            new_pool = copy(task_pool)
            new_pool.remove(task_to_do)
            if task_next[task_to_do] != -1:
                new_pool.append(task_next[task_to_do])
            end_task = cur_time + in_data['tasks']['time'][task_to_do]

            back_machine_t, back_ope_t = machine_end[machine_id], ope_end[ope_id]
            machine_end[machine_id], ope_end[ope_id] = end_task, end_task

            sub_score = explore_sol(in_data, cur_time, realease_order, last_realeased, task_next, new_pool, assigned_times, predict_depth+1, machine_end, ope_end, prev_task)
            if best_subtask_score is None or best_subtask_score < sub_score:
                best_subtask_score = sub_score
                best_subtask = (task_to_do, (machine_id, ope_id))

            # Back
            assigned_times[task_to_do] = -1
            machine_end[machine_id], ope_end[ope_id] = back_machine_t, back_ope_t

        if predict_depth == 0:
            return best_subtask_score, best_subtask, cur_time, last_realeased
        else:
            return best_subtask_score
    elif predict_depth < MAX_DEPTH and (len(task_pool) or last_realeased < len(realease_order)):
        return explore_sol(in_data, cur_time+1, realease_order, last_realeased, task_next, task_pool, assigned_times, predict_depth, machine_end, ope_end, prev_task)
    else:
        return scoring1(in_data, cur_time, assigned_times)

def generate_with_exploration(in_data):
    cur_time = 0
    realease_order = list(range(in_data['nb_jobs']))
    realease_order.sort(key = lambda job_id : in_data['jobs']['release'][job_id])
    last_realeased = 0
    task_next = [-1] * in_data['nb_tasks']
    prev_task = [-1] * in_data['nb_tasks']
    for tasks_list in in_data['jobs']['sequence']:
        for t1, t2 in zip(tasks_list, tasks_list[1:]):
            task_next[t1] = t2
            prev_task[t2] = t1
    task_pool = []
    assigned_times = [-1] * in_data['nb_tasks']
    assigned_to = [None] * in_data['nb_tasks']
    machine_end = [0] * in_data['nb_machines']
    ope_end = [0] * in_data['nb_operators']

    for _ in range(in_data['nb_tasks']):
        best_subtask_score, best_subtask, cur_time, last_realeased = explore_sol(in_data, cur_time, realease_order, last_realeased, task_next, task_pool, assigned_times, 0, machine_end, ope_end, prev_task)
        # print('best_subtask', best_subtask)

        task_to_do, (machine_id, ope_id) = best_subtask
        assigned_times[task_to_do] = cur_time
        assigned_to[task_to_do] = (machine_id, ope_id)
        end_task = cur_time + in_data['tasks']['time'][task_to_do]
        machine_end[machine_id], ope_end[ope_id] = end_task, end_task

        task_pool.remove(task_to_do)
        if task_next[task_to_do] != -1:
            task_pool.append(task_next[task_to_do])
    
    return {
        'task_to': assigned_to,
        'task_start': assigned_times,
    }