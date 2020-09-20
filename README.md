#SJoin

Experimental code for SIGMOD'20 paper: Efficient Join Synopsis
Maintenance for Data Warehouse. Includes 3 queries on TPC-DS data and
1 query on [Linear Road
dataset](https://github.com/walmartlabs/LinearGenerator).

## Compile

Requires a C++17 compiler (tested on GCC 8.3).

    cd sjoin
    mkdir release
    cd release
    CXXFLAGS=-O2 CPPFLAGS=-DNDEBUG ../configure
    make

## Prepare data

### TPC-DS

Download and run the [TPC-DS data generator](http://www.tpc.org/tpcds/).
Convert the data into formats required by QX, QY and QZ using tools
in tpcds_data_proc. Take QX as an example, compile and run

    ./tpcds_data_proc/create_qx_data ${path-to-tpcds-tbl-files} ${converted_data_file}

We tested SF = 1 and 10. Other SF should also work as long as they fit
in memory.

### Linear Road

Download and run the linear road data generator from Walmart
Lab(https://github.com/walmartlabs/LinearGenerator). Convert the data
using lroad_input_converter in sjoin.

    cd sjoin/release
    make lroad_input_converter
    ./lroad_input_converter ${lroad_data_file} ${converted_data_file} 0 1 3 F 60

## Run queries
All queries in the paper can be run using sjoin/release/driver. We've
replaced the hard-coded foreign key optimization with a more generic
query planner that recognizes foreign key specifications since the
experiments, so the resulting time and memory measurements may be
slightly different but are largely unchanged. See help for details.

    cd sjoin/release
    ./driver --help [<query_name>]

Valid query names include: tpcds_qx, tpcds_qy, tpcds_qz, linear_road.
Examples:

    ./driver -m qy.meter -a sjoin_opt -q tpcds_qy -i qy_data.in -o qy.out -s swor -r 10000
    ./driver -m lroad.meter -a sjoin_opt -q linear_road -i lroad_data.in -o lroad.out -s swor -r 10000 -w tumbling -t 60 3 300

## Format of input
SID is the stream ID. Tables are numbered sequentially from 0 in the
order that they are added to the query in the program (see
sjoin/src/tpcds_queries.cpp and sjoin/src/lroad_queries.cpp for details).
The following is the format of the (text) input:

    <SID>|<CVS_record>

e.g.,

    0|1,2,3

is a tuple (1, 2, 3) added to stream 0. It needs to be compatible
with the schema specified in the program.

### Null handling

Currently we don't have a bit in the record to indicate NULL values.
NULL values are usually treated as 0. TODO add the NULL bits

## Output

Per-window stats are printed to both the output file and stdout. If
the -n option is not given, tuple IDS of the samples are dumped to the
output file (but not stdout) following each per-window stats.

If the -m <meter-file> option is given, the number of tuples and
the number of join results processed in each 0.5 second period is
printed to the meter file.

## Contact

This code repository is open-sourced and may be used for research
purpose only. No express warranty of any kind and use at your own
risk.

Contact [Zhuoyue Zhao](zyzhao@cs.utah.edu) for any questions.

