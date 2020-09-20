#ifndef LINEAR_ROAD_H
#define LINEAR_ROAD_H

// Old hard-coded linear road implementation.
// The current implementation is in lroad_queries.h
// and lroad_queries.cpp.

#include <schema.h>
#include <string>

void run_linear_road(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width);

void run_linear_road_full(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width);

void run_linear_road_sliding_window(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width);

void run_linear_road_full_sliding_window(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width);

#endif

