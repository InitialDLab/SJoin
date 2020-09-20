// customer1 4
// customer2 4
// household_demographics1 5
// household_demographics2 5
// item1 15 
// item2 15
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

bool *is_varchar[7];
const int cids_of_item_sk_and_ticket_number[2] = {2, 9};
LL values_of_item_sk_and_ticket_number[2] = {0LL, 0LL};
vector<string> recs[7];

void init_is_varchar(const int col_nums[]) {
    for (int i = 0; i < 7; i++) {
        is_varchar[i] = new bool[col_nums[i]];
        for (int j = 0; j < col_nums[i]; j++) {
            is_varchar[i][j] = false;
        }
    }
    is_varchar[0][1] = true;
    is_varchar[1][1] = true;
    is_varchar[2][2] = true;
    is_varchar[3][2] = true;
    is_varchar[4][1] = is_varchar[4][4] = is_varchar[4][8] = is_varchar[4][10] = is_varchar[4][12] = is_varchar[4][14] = true;
    is_varchar[5][1] = is_varchar[5][4] = is_varchar[5][8] = is_varchar[5][10] = is_varchar[5][12] = is_varchar[5][14] = true;
}

int modify(int sid) {
    if (sid == 6) return 0;
    if (sid == 1) return 1;
    if (sid == 5) return 2;
    if (sid == 2) return 3;
    if (sid == 3) return 4;
    if (sid == 0) return 5;
    if (sid == 4) return 6; 
    return -1;
}

int main(int argc, char *argv[]) {
    default_random_engine e(time(0));

    /*if (argc < 16) {
        cout << "usage: " << argv[0] << " <customer1_infile> <customer1_#col> <customer2_infile> <customer2_#col> <household_demographics1_infile> <household_demographics1_#col> <household_demographics2_infile> <household_demographics2_#col> <item1_infile> <item1_#col> <item2_infile> <item2_#col> <store_sales_infile> <store_sales_#col> <outfile> " << endl;
        return 1;
    } */

    if (argc < 3) {
        cout << "usage: " << argv[0] << " <tpcds_data_dir> <outfile>" << endl;
        return 1;
    }

    std::string infile_name[7];
    const char *basenames[7] = {"customer.dat", "customer.dat",
        "household_demographics.dat", "household_demographics.dat",
        "item.dat", "item.dat", "store_sales.dat"};
    const int col_nums[7] = {4, 4, 5, 5, 15, 15, 11};
    string line;    

    for (int i = 0; i < 7; i++) {
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
    for (int i = 2; i < 5; i++) {
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
        for (int i = 0; i < 7; i ++) {
            if (i == 2 || i == 3 || i == 0 || i == 4) continue;
            total += recs[i].size();    
        }
        if (total == 0) break;
        int tmp = e() % total;
        int sid = -1;
        for (int i = 0; i < 7; i++) {
            if (i == 2 || i == 3 || i == 0 || i == 4) continue;
            if (tmp < recs[i].size()) {
                sid = i;
                break;
            }
            tmp -= recs[i].size();
        }
        line = recs[sid].back();
        recs[sid].pop_back();
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

    return 0;
}

