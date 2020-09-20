#!/bin/bash

CURDIR="`pwd`"
BASEDIR=$1
shift

cd "$BASEDIR"
if [ ! -d deps ]; then
    mkdir deps
    if [ ! -d deps/src ]; then
        mkdir deps/src
    fi

    if [ ! -d "deps/test" ]; then
        mkdir "deps/test"
    fi
fi

FILE_LIST="`find src -name '*.cpp'` `find test -name '*.cpp'`"
cd "$CURDIR"

for src_file in $FILE_LIST; do
    dirname=`dirname "$src_file"`
    basename=`basename "$src_file" | sed 's/[.]cpp//'`
    dep_filebase="$BASEDIR/deps/${dirname}/${basename}"
    $@ -MMD -MF "${dep_filebase}.d.raw" -E "$BASEDIR/${src_file}" > /dev/null
    sed "s,$BASEDIR/src,\$(srcdir),g" "${dep_filebase}.d.raw" |\
    sed "s,$BASEDIR/include,\$(includedir),g" |\
    sed "s,$BASEDIR/test,\$(testdir),g" |\
    sed "s,^\(.*[.]o\),\$(${dirname}bin)/\1," |\
    sed "s/config[.]h/\$(CONFIGS)/" |
    sed "s,$BASEDIR/config[.]top,,"|
    sed "s,$BASEDIR/config[.]bot,," > "${dep_filebase}.d" 
    echo -e '\t$(COMPILE) -o $@ $<\n' >> "${dep_filebase}.d"
done

if [ -f "$BASEDIR/deps/all_deps.d" ]; then
    mv "$BASEDIR/deps/all_deps.d" "$BASEDIR/deps/all_deps.d.bak"
fi

echo "" > "$BASEDIR/deps/all_deps.d"
cat `find "$BASEDIR/deps" -mindepth 2 -name '*.d'` >> "$BASEDIR/deps/all_deps.d"
echo "" >> "$BASEDIR/deps/all_deps.d"

OBJS=$(grep '^.*[.]o' "$BASEDIR/deps/all_deps.d" | \
    grep -v '^[$](testbin)' | grep -v input_converter | sed 's,:.*,,' | tr '\n' ' ')
echo $OBJS

cd "$BASEDIR"

mv Makefile.in Makefile.in.old

cat Makefile.in.old |
sed '
    /# objs/,/# end of objs/{
        /# end of objs/{
            r deps/all_deps.d
            N
        }
        /^#/!d
    }' |
sed "s,^OBJS=."'*'",OBJS=${OBJS}," > Makefile.in

#rm -f Makefile.in.old

