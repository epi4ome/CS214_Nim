#!/bin/bash
echo "== MATCH TEST =="

# Player 1
( printf "0|09|OPEN|Bob|"; sleep 1; \
  printf "0|09|MOVE|1|1|"; sleep 1; \
  printf "0|09|MOVE|3|5|" \
) | ./rawc localhost 15000 &

# Player 2
sleep 0.5
( printf "0|11|OPEN|Oskar|"; sleep 1; \
  printf "0|09|MOVE|2|3|"; sleep 1; \
  printf "0|09|MOVE|4|7|" \
) | ./rawc localhost 15000
