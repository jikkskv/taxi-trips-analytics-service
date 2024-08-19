@echo off
setlocal

set "fileId=https://drive.usercontent.google.com/download?id=1QLBGFOoKw_3-iM58q4unWfwHmPqfnrYr&export=download&authuser=0"
set "DESTINATION_DIR=\taxi-trips-analytics-rest\src\main\resources\dataset\taxitrips\trips_dats.parquet"


echo "Download starting, proceeding with setup..."
curl -sc /tmp/gcookie -L --insecure "%fileId%"

echo "Download complete, proceeding with setup..."