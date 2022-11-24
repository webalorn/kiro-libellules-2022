from util import *
import time
# TODO : should import functions from modules
import victor
import explorer

def generate_base_solution(in_data):
    return victor.process_very_basic(in_data)
    # return explorer.generate_with_exploration(in_data)
    #return {'empty' : True} # TODO : use functions from modules

def improve_sol(data):
    
    return data # TODO : use functions from modules

# ========== Main loop ==========

def main():
    inputs_names = INPUT_NAMES
    # If we want to tune only some solutions ->
    # inputs_names = ["medium.json"] 

    start_time = time.time()
    read_all_inputs()

    N_TRY_GENERATE = 10 # TODO : number of iterations
    # N_TRY_GENERATE = 1
    for name in inputs_names:
        print(f"========== GENERATE {name} ==========")
        in_data = IN_DATA[name]
        for k in range(N_TRY_GENERATE):
            print(f"{k+1}/{N_TRY_GENERATE}", end="\r")
            sol_data = generate_base_solution(in_data)
            output_sol_if_better(name, sol_data)

            # TODO: maybe improve a bit at first ?
            # Then output_sol_if_better(name, sol_data)
        print()
    
    # This will try to improve every solution stored in ../inputs
    N_TRY_IMPROVE = 10 # TODO : number of iterations
    for name in inputs_names:
        print(f"========== IMPROVE {name} ==========")
        in_data = IN_DATA[name]
        for k in range(N_TRY_IMPROVE):
            print(f"{k+1}/{N_TRY_IMPROVE}", end="\r")
            sol_data = improve_sol(BEST_SOLS_DATA[name])
            output_sol_if_better(name, sol_data)
        print()
    
    
    end_time = time.time()
    print(f"\n\nFinished for now, took {end_time-start_time:.2f}s")

if __name__ == "__main__":
    main()
