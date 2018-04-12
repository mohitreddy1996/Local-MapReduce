#include "map_reduce.h"

using namespace std;

#define MAX_SIZE_MAP_INPUT 7

// Master related private functions.
void MapReduce::master_assign_tasks_mapper() {
	close(master_to_mapper_pipe[0]); // Disable reading.
	int initial_task_count = mapper_count;
	while (1) {
		cout << "Assigning tasks to mapper..\n";
		string buffer = get_split_file_name(initial_task_count);
		write(master_to_mapper_pipe[1], buffer.c_str(), buffer.size());
		initial_task_count--;
		if (initial_task_count <= 0) {
			break;
		} else {
			sleep(1); // Pause before assigning another task.
		}
	}
	close(master_to_mapper_pipe[1]);
}

void MapReduce::master_wait_tasks_mapper() {
	close(mapper_to_master_pipe[1]);
	char *buff = (char *) calloc(0, sizeof(char));
	int read_size;
	int completed_tasks = 0;
	while ((read_size = read(mapper_to_master_pipe[0], buff, 1)) > 0) {
		cout << "Master waiting for mappers.. Mapper marked complete: " << buff << "\n";
		completed_tasks++;
		if (completed_tasks >= mapper_count) {
			break;
		} else {
			//sleep(1); // Pause before reading from pipe.
		}
	}
	free(buff);
	close(mapper_to_master_pipe[0]);
}

void MapReduce::master_group_by_keys() {
	string input_file_prefix = "logs/processed_map_";
	vector<pair<string, int> > string_int_pairs;
	for (int mapper_file_number = 0; mapper_file_number < mapper_count; mapper_file_number++) {
		string file_name = input_file_prefix + to_string(mapper_file_number);
		ifstream input_file(file_name);
		vector<string> string_list{istream_iterator<string>(input_file), {}};
		for (int string_pair_ind = 0; string_pair_ind < string_list.size(); string_pair_ind += 2) {
			string_int_pairs.push_back(make_pair(string_list[string_pair_ind], stoi(string_list[string_pair_ind + 1])));
		}
	}
	sort(string_int_pairs.begin(), string_int_pairs.end());

	string output_file_prefix = "logs/group_";
	ofstream output_file;
	int group_index = 0;
	int string_int_pairs_ind = 0;
	string string_name = "";
	while (string_int_pairs_ind < string_int_pairs.size()) {
		if (string_name != string_int_pairs[string_int_pairs_ind].first) {
			if (output_file.is_open()) {
				output_file.close();
			}
			output_file.open(output_file_prefix + to_string(group_index));
			group_index++;
		}
		output_file << string_int_pairs[string_int_pairs_ind].first << " " << to_string(string_int_pairs[string_int_pairs_ind].second) << "\n";
		string_name = string_int_pairs[string_int_pairs_ind].first;
		string_int_pairs_ind++;
	}
	if (output_file.is_open()) {
		output_file.close();
	}
}

void MapReduce::master_assign_tasks_reducer() {
	close(master_to_reducer_pipe[0]);
	int initial_task_count = reducer_count;
	while (1) {
		cout << "Assigning tasks to reducer.\n";
		string merge_split_file = get_merge_split_file_name(reducer_count - initial_task_count + 1);
		write(master_to_reducer_pipe[1], merge_split_file.c_str(), merge_split_file.size());
		initial_task_count--;
		if (initial_task_count <= 0) {
			break;
		} else {
			//sleep(1); // Wait before assigning another task.
		}
	}
	close(master_to_reducer_pipe[1]);
}

void MapReduce::master_wait_tasks_reducer() {
	close(reducer_to_master_pipe[1]);
	char *buff = (char *)calloc(0, sizeof(char));
	int temp;
	int completed_tasks = 0;
	while ((temp = read(reducer_to_master_pipe[0], buff, 1)) > 0) {
		cout << "Master \n";
		completed_tasks++;
		cout << "Completed task count: " << completed_tasks << " Reducer task count: " << reducer_count << "\n";
		if (completed_tasks >= reducer_count) {
			break;
		} else {
			sleep(1);
		}
	}
	free(buff);
	close(reducer_to_master_pipe[0]);
}

void MapReduce::master_merge_reducer_output() {
	string input_file_prefix = "logs/reduce_";
	string output_file = "reducer_output";
	ofstream output_file_;
	output_file_.open(output_file);
	for (int reducer_index = 0; reducer_index < reducer_count; reducer_index++) {
		ifstream input_(input_file_prefix + to_string(reducer_index));
		vector<string> string_list{istream_iterator<string>(input_), {}};
		output_file_ << string_list[0] << " " << string_list[1] << "\n";
	}
	output_file_.close();
}

void MapReduce::master_inform_parent_completion() {
	close(master_to_parent_pipe[0]);
	char *buff = "wake up.";
	cout << "Master invoking user routine: " << getpid() << " " << buff << "\n";
	write(master_to_parent_pipe[1], buff, strlen(buff));
	close(master_to_parent_pipe[1]);
}

void MapReduce::master_routine() {
	// assign tasks to mapper.
	master_assign_tasks_mapper();
	// wait for all mappers to complete.
	master_wait_tasks_mapper();
	// group by keys.
	master_group_by_keys();
	// assign tasks to reducers.
	master_assign_tasks_reducer();
	// wait for all reducers to complete.
	master_wait_tasks_reducer();
	// master merge all reducer outputs.
	master_merge_reducer_output();
	// inform parent process about completion.
	master_inform_parent_completion();
	cout << "Master process finished monitoring all tasks.\n";
}

// Mapper functions.

void MapReduce::map_(const string& fname) {
	process_map_file(fname);
}

void MapReduce::mapper_inform_master() {
	close(mapper_to_master_pipe[0]);
	char *buff = "Y";
	cout << "Mapper Pid: " << getpid() << " map operation done!\n";
	write(mapper_to_master_pipe[1], buff, strlen(buff));
	// Not closing the output pipe to master, it might be used again used somewhere.
}

void MapReduce::mapper_routine() {
	close(master_to_mapper_pipe[1]);
	char *buff = (char *) calloc(0, sizeof(char));

	int temp;

	while ((temp = read(master_to_mapper_pipe[0], buff, 1000)) > 0) {
		cout << "Mapper Pid: " << getpid() << " " << strlen(buff) << " char read from pipe: " << buff << "\n";
		map_(buff);
		mapper_inform_master();
	}
}

// Reducer private functions.

void MapReduce::reduce(char *fname) {
	reduce_file(string(fname));
}

void MapReduce::reducer_inform_master() {
	close(reducer_to_master_pipe[0]);
	char *buff = "Y";
	cout << "Reducer pid: " << getpid() << " reduce operation done.\n";
	write(reducer_to_master_pipe[1], buff, strlen(buff));
}

void MapReduce::reducer_routine() {
	close(master_to_reducer_pipe[1]);
	char *buff = (char *)calloc(0, sizeof(char));

	int temp;
	while ((temp = read(master_to_reducer_pipe[0], buff, 1000)) > 0) {
		cout << "Reducer pid: " << getpid() << " " << strlen(buff) << " chars read from pipe: " << buff << "\n";
		reduce(buff);
		reducer_inform_master();
	}
	cout << "Reducer pid: " << getpid() << " process finished. Informed master.\n";
	close(reducer_to_master_pipe[1]);
	close(master_to_reducer_pipe[0]);
}

// Parent related private functions.
void MapReduce::parent_routine() {
	// wait for parent process to inform parent process about completion.
	close(master_to_parent_pipe[1]);
	char *buff = (char *)calloc(0, sizeof(char));
	int temp;
	while ((temp = read(master_to_parent_pipe[0], buff, 1000)) > 0) {
		cout << "parent process " << strlen(buff) << " char from pipe: " << buff << "\n";
		break;
	}
	close(master_to_parent_pipe[0]);
}

MapReduce::MapReduce() {
	input_file = "";
}

MapReduce::MapReduce(const string& input_file) {
	this->input_file = input_file;
}

void MapReduce::create_mapper_input() {
	ifstream file_stream(input_file);
	map<string, int> string_count;
	vector<string> string_list{istream_iterator<string>(file_stream), {}};
	int strings_so_far = 1;
	ofstream map_files;
	string map_files_prefix = "logs/map_";
	string file_name = map_files_prefix + to_string(strings_so_far / MAX_SIZE_MAP_INPUT);
	//cout << "file_name: " << file_name << "\n";
	map_files.open(file_name);
	for (string input_str : string_list) {
		string_count[input_str]++;
		map_files << input_str + " ";
		if (strings_so_far % MAX_SIZE_MAP_INPUT == 0) {
			map_files.close();
			map_files.open(map_files_prefix + to_string(strings_so_far / MAX_SIZE_MAP_INPUT));
			cout << "file_name: " << map_files_prefix + to_string(strings_so_far / MAX_SIZE_MAP_INPUT) << "\n";
		}
		strings_so_far++;
	}
	if (map_files.is_open()) {
		map_files.close();
	}
	mapper_count = ceil((strings_so_far - 1) / MAX_SIZE_MAP_INPUT);
	reducer_count = string_count.size(); // HACK! Cant really implement auto scaling here.
	cout << "Mapper tasks: " << mapper_count << " " << "Reducer tasks: " << reducer_count << "\n";
}

void MapReduce::initialise_counters() {
	total_children = mapper_count + reducer_count + 1;
	child_index = 1;
}

void MapReduce::create_pipes() {
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

void MapReduce::start_map_reduce() {
	pid_t return_id = fork();
	if (return_id == 0) {
		// Child process.
		int process_id = getpid();
		if (child_index == total_children) {
			cout << "Master process (still a child) for mappers and reducers with pid: " << process_id << endl;
			master_routine();
		} else {
			if (child_index <= mapper_count) {
				// cout << "Mapper process with pid: " << process_id << endl;
				mapper_routine();
			} else {
				// cout << "Reducer process with pid: " << process_id << endl;
				reducer_routine();
			}
		}
	} else if (return_id > 0) {
		// Parent process.
		int process_id = getpid();
		// cout << "Parent prrocess with pid: " << process_id << endl;
		if (child_index < total_children) {
			child_index++;
			start_map_reduce();
		} else {
			parent_routine();
			cout << "Parent process finished.\n";
			wait();
			exit(0);
		}
	} else {
		// Error. Fork failed.
		cout << "Error while forking. Exiting...";
		exit(0);
	}
}