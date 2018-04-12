#include <bits/stdc++.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

string get_split_file_name(const int count_number);

void process_map_file(const string& fname);

string get_merge_split_file_name(const int count_number);

void reduce_file(const string& fname);