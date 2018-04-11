#include <bits/stdc++.h>
#include "map_reduce.h"

using namespace std;

int main(int argc, char **argv) {
	string input_file;
	if (argc < 2) {
		// default input file name.
		input_file = "input.txt";
	} else {
		// assuming the user will feed in the right input.
		// ToDo: Make this more robust.
		input_file = string(argv[1]);
	}
	MapReduce map_reduce(input_file);
	map_reduce.create_mapper_input();
	map_reduce.initialise_counters();
	map_reduce.create_pipes();
	map_reduce.start_map_reduce();
	return 0;
}