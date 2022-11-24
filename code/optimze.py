from util import *

def optimize_simple(in_data, out_data):
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
    
    job_last_at = [0] * in_data['nb_tasks']
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


#Look if it can replace a current couple for a task with another more interesting couple.
def optimize_changed_couple(in_data,out_data):
    out_data = out_data.deepcopy()

    for i in range(10000):
        mach_used = [[] for _ in range(in_data['nb_machines'])]
        op_used = [[] for _ in range (in_data['nb_operators'])]

        for id_task in range(in_data['nb_tasks']):
            debut_time = out_data['task_start'][id_task]
            machine = out_data['task_to'][id_task][0]
            op = out_data['task_to'][id_task][1]
            end_time = debut_time+in_data['tasks']['time'][id_task]
            mach_used[machine].append((debut_time,end_time,id_task))
            op_used[op].append((debut_time,end_time,id_task))

        for i in range(in_data['nb_operators']):
            op_used[i] = sorted(op_used[i])
        
        for i in range(in_data['nb_machines']):
            mach_used[i] = sorted(mach_used[i])

        nb_tasks = in_data['nb_tasks']

        id_task = randint(0,nb_tasks-1)

        m_curr,op_curr = out_data['task_to'][id_task]

        deb_curr, end_curr = (0,0)
        for deb,end,id in mach_used[m_curr]:
            if id == id_task:
                deb_curr = deb
                end_curr = end
                break

        l = in_data['tasks']['using'][id_task]

        possibleM = False
        possibleOp = False
        esperance = 0
        while not(possibleM) or not(possibleOp) and esperance < 10000:
            new_m,new_op = l[randint(0,len(l)-1)]
            esperance += 1
            if (new_m,new_op) != (m_curr,op_curr):
                for i in range(len(mach_used)-1):
                    if mach_used[new_m][i][1] <= deb_curr and mach_used[new_m][i+1][0] >= end_curr:
                        possibleM = True
                        break
                    if mach_used[new_m][i][0] >= end_curr:
                        break
                for i in range(len(op_used)-1):
                    if op_used[new_op][i][1] <= deb_curr and op_used[new_op][i+1][0] >= end_curr:
                        possibleOp = True
                        break
                    if op_used[new_op][i][0] >= end_curr:
                        break
        
        if possibleM and possibleOp:
            out_data['task_to'][id_task] = (new_m,new_op)
            out_data = optimize_simple(in_data,out_data)
    
    return out_data