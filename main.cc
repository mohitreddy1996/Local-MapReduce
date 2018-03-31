#include <bits/stdc++.h>
#include "mapper_helper.h"
using namespace std;

namespace map_reduce {

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
	int mappers_count = create_mappers_input(input_file);
	return 0;
}