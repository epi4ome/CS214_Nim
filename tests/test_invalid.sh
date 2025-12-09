#!/bin/bash
echo "== INVALID MESSAGE TESTS =="

echo "-- BAD HEADER (FAIL 10)"
printf "XX|11|OPEN|Bob|" | ./rawc localhost 15000

echo "-- MISSING TRAILING PIPE (FAIL 10)"
printf "0|11|OPEN|Bob" | ./rawc localhost 15000

echo "-- LONG NAME (FAIL 21)"
printf "0|80|OPEN|$(printf 'A%.0s' {1..80})|" | ./rawc localhost 15000

echo "-- ALREADY OPEN (FAIL 23)"
( printf "0|09|OPEN|Bob|"; sleep 0.2; printf "0|09|OPEN|Bob|" ) | ./rawc localhost 15000

echo "-- EARLY MOVE (FAIL 24)"
( printf "0|09|OPEN|Bob|"; sleep 0.2; printf "0|09|MOVE|1|1|" ) | ./rawc localhost 15000
