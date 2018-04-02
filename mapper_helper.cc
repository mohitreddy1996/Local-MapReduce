#include "mapper_helper.h"

using namespace std;

namespace map_reduce {
	int create_mapper_input(const string& input_file) {
		ifstream file_stream(input_file);
		vector<string> string_list{istream_iterator<string>(file_stream), {}};
		int strings_so_far = 1;
		ofstream map_files;
		string map_files_prefix = "map_";
		string file_name = map_files_prefix + to_str(strings_so_far / MAX_SIZE_MAP_INPUT);
		map_files.open(file_name);
		for (string input_str : string_list) {
			map_files << input_str + " ";
			if (strings_so_far % MAX_SIZE_MAP_INPUT == 0) {
				map_files.close();
				map_files.open(map_files_prefix + to_str(strings_so_far / MAX_SIZE_MAP_INPUT));
			}
			strings_so_far++;
		}
		if (map_files.is_open()) {
			map_files.close();
		}
		return ceil((strings_so_far - 1) / MAX_SIZE_MAP_INPUT);
	}
}