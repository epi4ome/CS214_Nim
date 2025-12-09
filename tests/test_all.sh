#!/bin/bash
echo "== RUNNING ALL TESTS =="

for t in test_basic.sh test_invalid.sh test_match.sh test_forfeit.sh
do
    echo ""
    echo "---------------------------------------"
    echo "Running $t"
    echo "---------------------------------------"
    bash ./$t
done
