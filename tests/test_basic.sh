#!/bin/bash
echo "== BASIC OPEN TEST =="

echo "Sending OPEN|Bob|"
printf "0|09|OPEN|Bob|" | ./rawc localhost 15000
