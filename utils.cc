#include "utils.h"

namespace {
	size_t split_string(const string &txt, vector<string> &strs, char ch) {
	    size_t pos = txt.find( ch );
	    size_t initialPos = 0;
	    strs.clear();

	    // Decompose statement
	    while( pos != string::npos ) {
	        strs.push_back( txt.substr( initialPos, pos - initialPos ) );
	        initialPos = pos + 1;

	        pos = txt.find( ch, initialPos );
	    }

	    // Add the last one
	    strs.push_back( txt.substr( initialPos, std::min( pos, txt.size() ) - initialPos + 1 ) );

	    return strs.size();
	}
}

string get_split_file_name(const int count_number) {
	return "logs/map_" + to_string(count_number - 1);
}

string get_merge_split_file_name(const int count_number) {
	return "logs/group_" + to_string(count_number - 1);
}

void process_map_file(const string& fname) {
	ifstream input_file(fname);
	vector<string> string_list{istream_iterator<string>(input_file), {}};
	vector<pair<string, int> > string_count;
	for (string input_ : string_list) {
		string_count.push_back(make_pair(input_, 1));
	}
	sort(string_count.begin(), string_count.end());
	ofstream output_file;
	string output_file_name = "logs/processed_map_";
	vector<string> split_file_name;
	split_string(fname, split_file_name, '_');
	output_file_name += split_file_name[split_file_name.size() - 1];
	output_file.open(output_file_name);
	for (auto string_count_pair : string_count) {
		output_file << string_count_pair.first << " " << to_string(string_count_pair.second) << "\n";
	}
	output_file.close();
}

void reduce_file(const string& fname) {
	ifstream input_file(fname);
	ofstream output_file;
	vector<string> string_list{istream_iterator<string>(input_file), {}};
	vector<string> split_file_name;
	split_string(fname, split_file_name, '_');
	string output_file_name = "logs/reduce_" + split_file_name[split_file_name.size() - 1];
	output_file.open(output_file_name);
	output_file << string_list[0] << " " << string_list.size() / 2 << "\n";
	output_file.close();
}