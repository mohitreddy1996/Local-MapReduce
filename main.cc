#include <bits/stdc++.h>
#include "mapper_helper.h"
using namespace std;

// global variables.

namespace map_reduce {
	// counters.
	int mapper_count; // Number of mapper tasks.
	int reducer_count; // Number of reducer tasks.
	int total_children; // Total number of children to be spawned. M + R + 1;

	// pipes.
	// interprocess communication. [0] represents read and [1] represents write.
	int parent_to_master_pipe[2]; // parent -> master.
	int master_to_parent_pipe[2]; // master -> parent.

	int master_to_mapper_pipe[2]; // master -> mapper.
	int mapper_to_master_pipe[2]; // mapper -> master.

	int master_to_reducer_pipe[2]; // master -> reducer.
	int reducer_to_master_pipe[2]; // reducer -> master.

	void set_mapper_count(int mapper_count_) {
		mapper_count = mapper_count_;
	}

	void initialise_counters() {
		reducer_count = mapper_count_ / 3; 
		total_children = mapper_count + reducer_count + 1;
	}

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
	map_reduce::set_mapper_count(mappers_count);
	map_reduce::initialise_counters();
	map_reduce::create_pipes();
	return 0;
}