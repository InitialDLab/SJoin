CXX=g++
CXXFLAGS=-O2

.PHONY: all clean
EXES=create_qx_data create_qy_data create_qz_data \
	 create_qy_wd_data

all: $(EXES)

create_qx_data: create_qx_data.cpp

create_qy_data: create_qy_data.cpp

create_qz_data: create_qz_data.cpp

create_qy_wd_data: create_qy_wd_data.cpp

clean:
	rm -f $(EXES)

