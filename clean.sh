#!/bin/bash
echo "SAMPLING_EVENT_ID,SAW_AGELAIUS_PHOENICEUS">>PredictedData.csv
cat PredictedData.csv
for filename in result/*; do
cat $filename >> PredictedData.csv
done


