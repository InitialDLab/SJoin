// customer1 4
// customer2 4
// household_demographics1 5
// household_demographics2 5
// store_sales 11
// and we also combine ss_ticket_number with ss_item_sk as ss_ticket_number_with_item_sk, which will be a new last column of store_sales in the form of (ss_item_sk << 32) + ss_ticket_number. 
// for now, we believe that 64-bit is enough to store this column


#include <iostream>
#include <fstream>
#include <random>
#include <vector>
#include <algorithm>
#include <cstring>

typedef long long LL;

using namespace std;

const int report_bar = 50000;
const int const_timestamp = 1;

bool *is_varchar[5];
const int cids_of_item_sk_and_ticket_number[2] = {2, 9};
LL values_of_item_sk_and_ticket_number[2] = {0LL, 0LL};
vector<string> recs[5];

void init_is_varchar(const int col_nums[]) {
    for (int i = 0; i < 5; i++) {
        is_varchar[i] = new bool[col_nums[i]];
        for (int j = 0; j < col_nums[i]; j++) {
            is_varchar[i][j] = false;
        }
    }
    is_varchar[0][1] = true;
    is_varchar[1][1] = true;
    is_varchar[2][2] = true;
    is_varchar[3][2] = true;
}

int modify(int sid) {
    if (sid == 0) return 4;
    if (sid == 4) return 0;
    return sid;
}

int main(int argc, char *argv[]) {
    default_random_engine e(time(0));

    if (argc < 3) {
        cout << "usage: " << argv[0] << " <tpcds_data_dir> <outfile>" << endl;
        return 1;
    }

    std::string infile_name[5];
    const char *basenames[5] = {
        "customer.dat", "customer.dat", "household_demographics.dat",
        "household_demographics.dat", "store_sales.dat"};
    const int col_nums[5] = {4, 4, 5, 5, 11};
    string line;    

    for (int i = 0; i < 5; i++) {
        infile_name[i] = std::string(argv[1]);
        if (!infile_name[i].empty() && infile_name[i][infile_name[i].length()] != '/') {
            infile_name[i].push_back('/');
        };
        infile_name[i].append(basenames[i]);
        std::ifstream fin(infile_name[i]);
        while (getline(fin, line), !line.empty()) {
            recs[i].push_back(line);
        }
        if (i == 1 || i == 0) continue; // customer 2 is in the same order of customer 1
        random_shuffle(recs[i].begin(), recs[i].end());
    }
    std::string outfile_name = argv[2];

    std::ofstream fout(outfile_name);

    init_is_varchar(col_nums);

    int line_id = 0;
    for (int i = 2; i < 4; i++) {
        cout << "Processsing streaming " << i << endl;
        for (int j = 0; j < recs[i].size(); j++) {
            line = recs[i][j];
            if (line_id % report_bar == 0) fout << "-" << endl;
            const char *cline = line.c_str();
            fout << modify(i) << "|" << const_timestamp << ",";
            bool flag = false;
            int colid = 0;
            if (is_varchar[i][colid]) fout <<"\"";
            while (*cline) {
                if (flag) {
                    flag = false;
                    if (is_varchar[i][colid - 1]) fout << "\"";
                    fout << ",";
                    if (is_varchar[i][colid]) fout << "\"";
                }
                if (*cline == '|') {
                    flag = true;
                    ++ colid;
                    if (colid >= col_nums[i]) break;
                    ++ cline;
                    continue;
                }
                fout << *cline;
                ++ cline;
            }
            if (is_varchar[i][col_nums[i] - 1]) fout << "\"";
            fout << endl;
            ++ line_id;
            if (line_id % 10000 == 0) {
                cout << i << " : " << line_id << endl;
            }
        }
    }
    for (int i = 0; i < 1; i++) {
        cout << "Processsing streaming " << i << endl;
        for (int j = 0; j < recs[i].size(); j++) {
            line = recs[i][recs[i].size() - j - 1];
            if (line_id % report_bar == 0) fout << "-" << endl;
            const char *cline = line.c_str();
            fout << modify(i) << "|" << const_timestamp << ",";
            bool flag = false;
            int colid = 0;
            if (is_varchar[i][colid]) fout <<"\"";
            while (*cline) {
                if (flag) {
                    flag = false;
                    if (is_varchar[i][colid - 1]) fout << "\"";
                    fout << ",";
                    if (is_varchar[i][colid]) fout << "\"";
                }
                if (*cline == '|') {
                    flag = true;
                    ++ colid;
                    if (colid >= col_nums[i]) break;
                    ++ cline;
                    continue;
                }
                fout << *cline;
                ++ cline;
            }
            if (is_varchar[i][col_nums[i] - 1]) fout << "\"";
            fout << endl;
            ++ line_id;
            if (line_id % 10000 == 0) {
                cout << i << " : " << line_id << endl;
            }
        }
    }
    while (true) {
        int total = 0;
        for (int i = 0; i < 5; i ++) {
            if (i == 2 || i == 3 || i == 0) continue;
            total += recs[i].size();    
        }
        if (total == 0) break;
        int tmp = e() % total;
        if (tmp < recs[1].size()) {
            line = recs[1].back();
            recs[1].pop_back();
            for (int sid = 1; sid <= 1; sid ++) {
                if (line_id % report_bar == 0) fout << "-" << endl;
                const char *cline = line.c_str();
                fout << modify(sid) << "|" << const_timestamp << ",";
                bool flag = false;
                int colid = 0;
                values_of_item_sk_and_ticket_number[0] = values_of_item_sk_and_ticket_number[1] = 0LL;
                if (is_varchar[sid][colid]) fout <<"\"";
                while (*cline) {
                    if (flag) {
                        flag = false;
                        if (is_varchar[sid][colid - 1]) fout << "\"";
                        fout << ",";
                        if (is_varchar[sid][colid]) fout << "\"";
                    }
                    if (*cline == '|') {
                        flag = true;
                        ++ colid;
                        if (colid >= col_nums[sid]) break;
                        ++ cline;
                        continue;
                    }
                    fout << *cline;
                    if (colid == cids_of_item_sk_and_ticket_number[0]) {
                        values_of_item_sk_and_ticket_number[0] *= 10;
                        values_of_item_sk_and_ticket_number[0] += *cline - '0';
                    } else if (colid == cids_of_item_sk_and_ticket_number[1]) {
                        values_of_item_sk_and_ticket_number[1] *= 10;
                        values_of_item_sk_and_ticket_number[1] += *cline - '0';
                    }
                    ++ cline;
                }
                if (is_varchar[sid][col_nums[sid] - 1]) fout << "\"";
                fout << endl;
                ++ line_id;
                if (line_id % 10000 == 0) {
                    cout << sid << " : " << line_id << endl;
                }
            }
        } else {
            int sid = 4;
            line = recs[4].back();
            recs[4].pop_back();
            if (line_id % report_bar == 0) fout << "-" << endl;
            const char *cline = line.c_str();
            fout << modify(sid) << "|" << const_timestamp << ",";
            bool flag = false;
            int colid = 0;
            values_of_item_sk_and_ticket_number[0] = values_of_item_sk_and_ticket_number[1] = 0LL;
            if (is_varchar[sid][colid]) fout <<"\"";
            while (*cline) {
                if (flag) {
                    flag = false;
                    if (is_varchar[sid][colid - 1]) fout << "\"";
                    fout << ",";
                    if (is_varchar[sid][colid]) fout << "\"";
                }
                if (*cline == '|') {
                    flag = true;
                    ++ colid;
                    if (colid >= col_nums[sid]) break;
                    ++ cline;
                    continue;
                }
                fout << *cline;
                if (colid == cids_of_item_sk_and_ticket_number[0]) {
                    values_of_item_sk_and_ticket_number[0] *= 10;
                    values_of_item_sk_and_ticket_number[0] += *cline - '0';
                } else if (colid == cids_of_item_sk_and_ticket_number[1]) {
                    values_of_item_sk_and_ticket_number[1] *= 10;
                    values_of_item_sk_and_ticket_number[1] += *cline - '0';
                }
                ++ cline;
            }
            if (is_varchar[sid][col_nums[sid] - 1]) fout << "\"";
            fout << ",";
            fout << ((values_of_item_sk_and_ticket_number[0] << 32) + values_of_item_sk_and_ticket_number[1]);
            fout << endl;
            ++ line_id;
            if (line_id % 10000 == 0) {
                cout << sid << " : " << line_id << endl;
            }
        }

    }

    return 0;
}

