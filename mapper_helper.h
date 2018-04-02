#include <bits/stdc++.h>

using namespace std;

#define MAX_SIZE_MAP_INPUT 7

namespace map_reduce {
	/**
	* Get number of map tasks required from the size of the input file.
	* Also create the split files to be used by map tasks.
	*/
	int create_mapper_input(const string& input_file);
}