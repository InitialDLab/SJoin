#include <config.h>
#include <lroad_queries.h>
#include <linear_road_schema.h>

#include <iostream>
#include <string>

void print_linear_road_help() {
    std::cerr << "Required additional args for linear_raod: "
                 "<nlanes> <join_cond_half_bandwidth>" << std::endl;
}


int setup_linear_road_query(
    Query *query,
    const std::vector<std::string> &additional_args) {
    
    if (additional_args.size() < 2) {
        std::cerr << "[ERROR] missing additional args for linear road"
            << std::endl;
        print_linear_road_help();
        return 1;
    }
    
    errno = 0;
    int nlanes = (int) strtoul(additional_args[0].c_str(), nullptr, 0);
    if (errno != 0) {
        std::cerr << "[ERROR] invalid nlanes: " << additional_args[0]
            << std::endl;
        return 1;
    }
    
    int band_width_value =
        (int) strtoul(additional_args[1].c_str(), nullptr, 0);
    if (errno != 0) {
        std::cerr << "[ERROR] invalid join_half_bandwidth: "
            << additional_args[1] << std::endl;
        return 1;
    }
    DATUM band_width = DATUM_from_INT4(band_width_value);
    
    if (query->is_unwindowed()) {
        std::cerr << "[WARN] unwindowed band join is extremely large and "
            "not tested" << std::endl;
        if (query->allow_deletion()) {
            std::cerr << "[WARN] not expecting -d/--allow-deletion for "
                "unwindowed linear road query\n"
                "NOTE We currently only allow deletion in the same "
                "order as insertion.\n"
                "NOTE You might want to make sure your data deletes "
                "tuples in FIFO order.\n"
                "FIXME we should allow out-of-order deletion by "
                "allow search in the node tuple ID list in the future."
                << std::endl;
        }
    }

    Schema *sch = get_linear_road_schema();
    COLID pos_colid = sch->get_colid_from_name("pos");
    RT_ASSERT(pos_colid != INVALID_COLID);
    RT_ASSERT(sch->get_ts_colid() != INVALID_COLID);

    for (int i = 0; i < nlanes; ++i) {
        SID sid = query->add_stream(sch);
        if (i != 0) {
            query->add_band_join(
                sid - 1,
                pos_colid,
                sid,
                pos_colid,
                &band_width);
        }
    }

    return 0;
}

