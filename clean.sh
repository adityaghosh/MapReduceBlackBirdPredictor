#!/bin/bash

declare -i cnt=0

while [ $cnt -lt 13 ]
do
if  [ $cnt -lt 10 ] 
then
cat "part-0000"$cnt >> clean.csv
else
cat "part-000"$cnt >> clean.csv
fi
cnt=$cnt+1
done

