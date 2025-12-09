#!/bin/bash

echo "== FORFEIT TEST =="

# Player 1 connects, then quits immediately
printf "0|09|OPEN|Bob|" | ./rawc localhost 15000 &

sleep 1

# Player 2 should get NAME then OVER
printf "0|11|OPEN|Oskar|" | ./rawc localhost 15000

sleep 1
