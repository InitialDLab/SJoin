#include <config.h>
#include <schema.h>
#include <record.h>
#include <linear_road_schema.h>
#include <memmgr.h>
#include <iostream>
#include <fstream>
#include <random>
using namespace std;

int main(int argc, char *argv[]) {
    if (argc < 8) {
        cout << "usage: " << argv[0] <<
            " <infile> <outfile> <Dir> <MinLane> <MaxLane> <Type:F/P> <FixedInterval/InvProbReport>"
            << endl;
        return 1;
    }

    create_root_memmgrs();
    init_typeinfos();
    
    std::string infile_name = argv[1];
    std::string outfile_name = argv[2];
    int dir0 = atoi(argv[3]);
    int min_lane = atoi(argv[4]);
    int max_lane = atoi(argv[5]);
    bool is_fixed_interval = argv[6][0] == 'F';
    UINT8 param = strtoull(argv[7], nullptr, 0);

    std::ifstream fin(infile_name);
    std::ofstream fout(outfile_name);
    std::string line;

    Schema *sch = get_linear_road_schema();
    BYTE *buf = g_tracked_mmgr->allocate_array<BYTE>(sch->get_max_len());
    Record rec;
    rec.set_schema(sch);
    rec.set_buf(buf, sch->get_max_len());
    
    COLID ts_colid = sch->get_ts_colid();
    COLID type_colid = sch->get_colid_from_name("type");
    COLID dir_colid = sch->get_colid_from_name("dir");
    COLID lane_colid = sch->get_colid_from_name("lane");
        
    TIMESTAMP report_boundary = (TIMESTAMP) param;
    
    std::random_device rdev;
    std::mt19937_64 rng(rdev());
    std::uniform_int_distribution<UINT8> unif(0, param - 1);
    
    while (getline(fin, line), !line.empty()) {
        rec.parse_line(line.c_str(), ',');
        
        int type = rec.get_int(type_colid);
        int dir = rec.get_int(dir_colid);
        int lane = rec.get_int(lane_colid);
        TIMESTAMP ts = rec.get_long(ts_colid);

        if (type != 0 || dir != dir0 || lane < min_lane || lane > max_lane) {
            continue;
        }

        if (is_fixed_interval) {
            if (ts >= report_boundary) {
                fout << '-' << endl;
                report_boundary = (ts / param + 1) * param;
            }
        } else {
            if (unif(rng) == 0) {
                fout << '-' << endl;
            }
        }
        
        fout << lane - min_lane << '|' << line << endl;
    }
    
    // always place a report event at the very end
    fout << '-' << endl;
    fin.close();
    fout.close();

    destroy_root_memmgrs();

    return 0;
}

