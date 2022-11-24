import copy import deepcopy
from util import *

def optimize_simple(in_data, out_data):
    out_data = deepcopy(out_data)
    machine_at = set()
    ope_at = set()
    sorted_task_ids = task_ids_sorted_by_time(out_data['time'])
    
    for task_id in sorted_task_ids:
        task_t = out_data['time'][task_id]
        task_last_t = out_data['time'][task_id] + in_data['tasks']['time'][task_id]-1
        a_machine, a_ope = out_data['task_to']
        machine_at.add((a_machine, task_t))
        ope_at.add((a_ope, task_t))
        machine_at.add((a_machine, task_last_t))
        ope_at.add((a_ope, task_last_t))
    
    job_last_at = [t0 for t0 in in_data['jobs']['release']]
    for task_id in sorted_task_ids:
        task_t = out_data['time'][task_id]
        durm1 = in_data['tasks']['time'][task_id]-1
        task_last_t = out_data['time'][task_id] + durm1
        a_machine, a_ope = out_data['task_to']

        machine_at.remove((a_machine, task_t))
        ope_at.remove((a_ope, task_t))
        if durm1:
            machine_at.remove((a_machine, task_last_t))
            ope_at.remove((a_ope, task_last_t))

        for t0 in range(job_last_at[in_data['job_id'][task_id]], task_t-1):
            t0_last = t0 + durm1
            for (a2_machine, a2_ope) in in_data['tasks']['using'][task_id]:
                if ((a2_machine, t0) not in machine_at
                    and (a2_ope, t0) not in ope_at
                    and (a2_machine, t0_last) not in machine_at
                    and (a2_ope, t0_last) not in ope_at):
                    a_machine, a_ope, task_t, task_last_t = a2_machine, a2_ope, t0, t0_last
                    out_data['task_to'] = (a2_machine, a2_ope)
                    out_data['task_start'] = t0
                    break
        
        machine_at.add((a_machine, t0))
        ope_at.add((a_ope, t0))
        if durm1:
            machine_at.add((a_machine, t0_last))
            ope_at.add((a_ope, t0_last))
        
        job_last_at[in_data['job_id'][task_id]] = task_last_t+1
    return out_data