#include <bits/stdc++.h>

using namespace std;

int main() {
	string input_file = "../input.txt";
	ifstream file_stream(input_file);
	vector<string> string_list{istream_iterator<string>(file_stream), {}};
	for (string str : string_list) {
		cout << str << " ";
	}
	return 0;
}