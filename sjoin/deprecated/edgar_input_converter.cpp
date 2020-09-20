#include <config.h>
#include <schema.h>
#include <record.h>
#include <memmgr.h>
#include <dtypes.h>
#include <iostream>
#include <fstream>
#include <std_containers.h>
#include <algorithm>
#include <ctime>
#include <random>
#include <sstream>
using namespace std;

std_unordered_map<std::string, UINT4> *
load_octet_mapping(char *mapping_file, std_vector<UINT4> &unused_octet) {
    std_unordered_map<std::string, UINT4> *octet_mapping = g_tracked_mmgr->newobj<
        std_unordered_map<std::string, UINT4>>(g_tracked_mmgr);
    unused_octet.clear();
    unused_octet.resize(256, 0);

    if (mapping_file) {
        std::string line;
        std::ifstream fin(mapping_file);
        while (std::getline(fin, line), !line.empty()) {
            UINT4 octet = (UINT4) stoll(line.substr(4, 3));
            octet_mapping->operator[](line.substr(0, 3)) = octet;
            unused_octet[octet] = 1;
        }
    }
    
    UINT4 j = 0;
    for (UINT4 i = 0; i < 256u; ++i) {
        if (unused_octet[i] == 0) {
            unused_octet[j++] = i;
        }
    }
    unused_octet.resize(j);

    std::random_device rdev;
    std::mt19937_64 rng(rdev());
    std::shuffle(unused_octet.begin(), unused_octet.end(), rng);

    return octet_mapping;
}

UINT8
get_mapped_octet(
    std::string obfuscated_octet,
    std_unordered_map<std::string, UINT4> *mapping,
    std_vector<UINT4> &unused_octet) {
    
    auto iter = mapping->find(obfuscated_octet);
    if (iter != mapping->end()) {
        return iter->second;
    } 

    UINT4 octet = unused_octet.back();
    unused_octet.pop_back();
    mapping->emplace(obfuscated_octet, octet);
    return octet;
}

void
dump_mapping_file(
    std_unordered_map<std::string, UINT4> *mapping,
    char *mapping_file) {

    if (!mapping_file) return ;

    std_vector<std::pair<std::string, UINT4>> mapping_pairs(
        g_root_mmgr, mapping->begin(), mapping->end());
    
    std::sort(mapping_pairs.begin(), mapping_pairs.end(),
        [](const std::pair<std::string, UINT4> &p1,
           const std::pair<std::string, UINT4> &p2) -> bool {
            return p1.second < p2.second;
        });
    
    std::ofstream fout(mapping_file);
    for (const auto &p : mapping_pairs) {
        fout << p.first << ' ' << p.second << endl;
    }
    fout.close();
}

int convert(int argc, char *argv[]) {
    if (argc < 5) {
        cout << "usage: " << argv[0]
            << " <infile> <outfile> <Type:F/P> <FixedInterval/InvProbReport> [mappingFile]"
            << endl;
        return 1;
    }

    std::string infile_name = argv[1];
    std::string outfile_name = argv[2];
    bool is_fixed_interval = argv[3][0] == 'F';
    UINT8 param = strtoull(argv[4], nullptr, 0);
    char *mapping_file = nullptr;
    if (argc >= 6) {
        mapping_file = argv[5];
    }

    std_vector<UINT4> unused_octet(g_tracked_mmgr);
    std_unordered_map<std::string, UINT4> *
        octet_mapping = load_octet_mapping(argv[5], unused_octet);

    const char *names[] = {
        "ip",
        "date",
        "time",
        "zone",
        "cik",
        "accession",
        "doc",
        "code",
        "size",
        "idx",
        "norefer",
        "noagent",
        "find",
        "crawler",
        "browser"
    };
    typeinfo *ti[] = {
        g_tracked_mmgr->newobj<typeinfo_varchar>(15),
        get_fixedlen_ti(DTYPE_DATE),
        g_tracked_mmgr->newobj<typeinfo_varchar>(8),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_DOUBLE),
        g_tracked_mmgr->newobj<typeinfo_varchar>(20),
        g_tracked_mmgr->newobj<typeinfo_varchar>(100),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_LONG),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        g_tracked_mmgr->newobj<typeinfo_varchar>(3)
    };
    Schema *sch = g_tracked_mmgr->newobj<Schema>(
        g_tracked_mmgr,
        15,
        ti,
        names);

    BYTE *buf = g_tracked_mmgr->allocate_array<BYTE>(sch->get_max_len());
    Record rec;
    rec.set_schema(sch);
    rec.set_buf(buf, sch->get_max_len());

    COLID ip_colid = sch->get_colid_from_name("ip");
    COLID date_colid = sch->get_colid_from_name("date");
    COLID time_colid = sch->get_colid_from_name("time");
    COLID zone_colid = sch->get_colid_from_name("zone");

    TIMESTAMP report_boundary = 0;
    std::random_device rdev;
    std::mt19937_64 rng(rdev());
    std::uniform_int_distribution<UINT8> unif(0, param - 1);
    
    TIMESTAMP last_ts = 0;
    std_vector<std::pair<time_t, std::string>> d1(g_root_mmgr),
        d2(g_root_mmgr), d3(g_root_mmgr);

    std::ifstream fin(infile_name);
    std::ofstream fout(outfile_name);
    std::string line;
    while (std::getline(fin, line), !line.empty()) {
        rec.parse_line(line.c_str(), ',');
        
        PAGE_OFFSET ip_len;
        char *ip_str = rec.get_varchar(ip_colid, &ip_len);
        char *ip_str_end = ip_str + ip_len;
        UINT4 ip = 0;
        int i = 0;
        while (i < 3) {
            UINT4 octet = 0;
            while (ip_str != ip_str_end && *ip_str != '.') {
                octet = octet * 10 + *ip_str - '0';
                ++ip_str;
            }
            ip = ip | (octet << ((3 - i) * 8));
            ++i;
            if (ip_str != ip_str_end) ++ip_str; else break;
        }
        if (i == 3 && ip_str != ip_str_end) {
            std::string last_obfuscated_octet(ip_str, ip_str_end - ip_str);
            UINT4 last_octet = get_mapped_octet(
                last_obfuscated_octet,
                octet_mapping,
                unused_octet);
            ip = ip | last_octet;
        }
        
        PAGE_OFFSET time_len;
        char *time_c_str = rec.get_varchar(time_colid, &time_len);
        std::string time(time_c_str, time_len);
        RT_ASSERT(time_len == 8);
        UINT4 date = rec.get_date(date_colid);
        tm t_tm;
        t_tm.tm_sec = stoi(time.substr(6, 2));
        t_tm.tm_min = stoi(time.substr(3, 2));
        t_tm.tm_hour = stoi(time.substr(0, 2));
        t_tm.tm_mday = (int)((date & 0x1f) + 1);
        t_tm.tm_mon = (int)((date >> 5) & 0xf);
        t_tm.tm_year = (int)((date >> 9) - 1900);
        t_tm.tm_isdst = -1;
        std::time_t secs_since_epoch = std::mktime(&t_tm);
        std::string data_str(10, '\0');
        snprintf(data_str.data(), 11, "%04d-%02d-%02d",
            t_tm.tm_year + 1900, t_tm.tm_mon + 1, t_tm.tm_mday);
        
        if (is_fixed_interval) {
            if (report_boundary == 0) {
                report_boundary = (secs_since_epoch + param) / param * param;
            }
            if (secs_since_epoch >= report_boundary) {
                fout << '-' << endl;
                report_boundary = (secs_since_epoch / param + 1) * param;
            }
        } else {
            if (unif(rng) == 0) {
                fout << '-' << endl;
            }
        }
        
        if (last_ts < secs_since_epoch) {
            RT_ASSERT(d1.empty() || last_ts == d1.front().first);

            auto i1 = d1.begin(), e1 = d1.end();
            auto i2 = d2.begin(), e2 = d2.end();
            auto i3 = d3.begin(), e3 = d3.end();
            while (i1 != e1 || i2 != e2 || i3 != e3) {
                if (i1 != e1) {
                    fout << "0|" << i1->first << ',' << i1->second << endl;
                    ++i1;
                }
                if (i2 != e2) {
                    fout << "1|" << i2->first + 1 << ',' << i2->second << endl;
                    ++i2;
                }
                if (i3 != e3) {
                    fout << "2|" << i3->first + 2 << ',' << i3->second << endl;
                    ++i3;
                }
            }
            if (last_ts + 1 == secs_since_epoch) {
                d3 = std::move(d2);
                d2 = std::move(d1);
                d1.clear();
            } else if (last_ts + 2 == secs_since_epoch) {
                d3 = std::move(d1);
                d2.clear();
                d1.clear();
            } else {
                d3.clear();
                d2.clear();
                d1.clear();
            }

            last_ts = secs_since_epoch;
        }

        std::ostringstream out;
        out << ip << ',' << (ip >> 8) << ',' << secs_since_epoch;
        out << ',' << data_str << ',' << time;
        for (COLID colid = zone_colid; colid < sch->get_num_cols(); ++colid) {
            auto str = rec.to_string(colid);
            if (str.find(',') != std::string::npos) {
                out << ",\"" << str << '"';
            } else {
                out << ',' << str;
            }
        }

        d1.emplace_back(secs_since_epoch, out.str());
    }

    fout << '-' << endl; // add an additional report at the end of day
    
    fout.close();

    dump_mapping_file(octet_mapping, mapping_file);

    return 0;
}

int main(int argc, char *argv[]) {
    create_root_memmgrs();
    init_typeinfos();
    
    int ret_code = convert(argc, argv);

    destroy_root_memmgrs();

    return ret_code;
}

