#include <bits/stdc++.h>
#include <map>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "utils.h"

using namespace std;

class MapReduce {
private:
	string input_file;
	// counters.
	int mapper_count; // Number of mapper tasks.
	int reducer_count; // Number of reducer tasks.
	int total_children; // Total number of children to be spawned. M + R + 1;
	int child_index;
	// pipes.
	// interprocess communication. [0] represents read and [1] represents write.
	int parent_to_master_pipe[2]; // parent -> master.
	int master_to_parent_pipe[2]; // master -> parent.

	int master_to_mapper_pipe[2]; // master -> mapper.
	int mapper_to_master_pipe[2]; // mapper -> master.

	int master_to_reducer_pipe[2]; // master -> reducer.
	int reducer_to_master_pipe[2]; // reducer -> master.

	// private member functions.
	// master related functions.

	/*
	* Master assigns split files to mapper tasks.
	*/
	void master_assign_tasks_mapper();

	/*
	* Master wait for all the mappers to finish the task and report.
	*/
	void master_wait_tasks_mapper();

	/*
	* Master assigns files to be processed to reducer tasks.
	*/
	void master_assign_tasks_reducer();

	/*
	* Master wait for reducer tasks to complete.
	*/
	void master_wait_tasks_reducer();

	/*
	* Master informs parent process regarding completion of full map reduce task.
	*/
	void master_inform_parent_completion();

	/*
	* Master group by keys and put them in different files.
	*/
	void master_group_by_keys();

	/*
	* Master group outputs of reducers.
	*/
	void master_merge_reducer_output();

	/*
	* Assign tasks to mappers, wait for completion, assign tasks
	* to reducer, wait for completion, inform parent.
	*/
	void master_routine();

	// Mapper related private functions.

	/*
	* Perform mapping.
	*/
	void map_(const string& fname);

	/*
	* Inform master regarding completion.
	*/
	void mapper_inform_master();

	/*
	* Ask each mapper process to perform map task and inform user.
	*/
	void mapper_routine();

	// Reducer private functions.

	/*
	* Perform reducer operation.
	*/
	void reduce(char *fname);

	/*
	* Reducer inform master.
	*/
	void reducer_inform_master();

	/*
	* Ask reducer tasks to start reducing tasks.
	*/
	void reducer_routine();	

	// user routine.
	/*
	* Ask parent to start the fork and assigning tasks.
	*/
	void parent_routine();

public:
	MapReduce();
	MapReduce(const string& input_file);

	void create_mapper_input();
	/*
	* Set reducer_count = mapper_count / 3. Could be any fraction.
	*/
	void initialise_counters();

	/*
	* Create pipes for inter process communication.
	*/
	void create_pipes();

	/*
	* Fork children and ask mappers and reducers.
	*/
	void start_map_reduce();
};