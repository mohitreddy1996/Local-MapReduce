#include <bits/stdc++.h>
#include "mapper_helper.h"
#include "parent_helper.h"
#include "master_helper.h"
#include "reducer_helper.h"

using namespace std;

// global variables.

namespace map_reduce {
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

	/*
	* Set mapper count to mapper_count in map_reduce namespace.
	*/
	void set_mapper_count(int mapper_count_) {
		mapper_count = mapper_count_;
	}

	/*
	* Set reducer_count = mapper_count / 3. Could be any fraction.
	*/
	void initialise_counters() {
		reducer_count = mapper_count_ / 3; 
		total_children = mapper_count + reducer_count + 1;
		child_index = 1;
	}

	/*
	* Create pipes for inter process communication.
	*/
	void create_pipes() {
		if (pipe(master_to_parent_pipe) < 0) {
			cout << "Master to parent pipe creation error.\n";
			exit(1);
		}

		if (pipe(parent_to_master_pipe) < 0) {
			cout << "Parent to master pipe creation error.\n";
			exit(1);
		}

		if (pipe(master_to_mapper_pipe) < 0) {
			cout << "Master to mapper pipe creation error.\n";
			exit(1);
		}

		if (pipe(mapper_to_master_pipe) < 0) {
			cout << "Mapper to master pipe creation error.\n";
			exit(1);
		}

		if (pipe(master_to_reducer_pipe) < 0) {
			cout << "Master to reducer pipe creation error.\n";
			exit(1);
		}

		if (pipe(reducer_to_master_pipe) < 0) {
			cout << "Reducer to master pipe creation error.\n";
			exit(1);
		}
	}

	/*
	* Fork children and ask mappers and reducers.
	*/
	void start_map_reduce() {
		pid_t return_id = fork();
		if (return_id == 0) {
			// Child process.
			int process_id = getpid();
			if (child_index == total_children) {
				cout << "Master process (still a child) for mappers and reducers with pid: " << process_id << endl;
				master_routine();
			} else {
				if (child_index <= mapper_count) {
					cout << "Mapper process with pid: " << process_id << endl;
					mapper_routine();
				} else {
					cout << "Reducer process with pid: " << process_id << endl;
					reducer_routine();
				}
			}
		} else if (return_id > 0) {
			// Parent process.
			int process_id = getpid();
			cout << "Parent prrocess with pid: " << process_id << endl;
			if (child_index < total_children) {
				child_index++;
				start_map_reduce();
			} else {
				parent_routine();
				cout << "Parent process finished.\n";
				wait(NULL);
				exit(0);
			}
		} else {
			// Error. Fork failed.
			cout << "Error while forking. Exiting...";
			exit(0);
		}
	}
}

int main(int argc, void **argv) {
	string input_file;
	if (argc < 2) {
		// default input file name.
		input_file = "input.txt";
	} else {
		// assuming the user will feed in the right input.
		// ToDo: Make this more robust.
		input_file = argv[1];
	}
	int mapper_count = map_reduce::create_mapper_input(input_file);
	map_reduce::set_mapper_count(mapper_count);
	map_reduce::initialise_counters();
	map_reduce::create_pipes();
	map_reduce::start_map_reduce();
	return 0;
}