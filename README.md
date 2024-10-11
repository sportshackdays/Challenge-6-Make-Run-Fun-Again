# Challenge-6-Make-Run-Fun-Again

The gpx folder contains 3 gpx files drawn using the https://schweizmobil.ch website
The Murtenlauf 17.17km route can be found in the "murtenlauf1717.gpx" 

The runs data contains sensor data from 4 runners who completed the various tracks

## cleaning and preprocessing of the input data
the python files change_date.py and cleaner.py were used to prepare the data input files. You do not have to do that again.
The files to be used for the simulator are the *cleaned.csv" files.

### change_date.py
This file modifies the epoc time of the data file "dump_300857_2024_10_06T07_45_00_000000Z.csv.ori" to be closer to the other runners.
300857 -> epoc 1728202247 -> Sunday, October 6, 2024 10:10:48 AM GMT+02:00 DST --> 1728201795 (Sunday, October 6, 2024 10:03:15 AM GMT+02:00)
1728202247 - 1728201795 = 452
301612 -> epoc 1728201784 -> Sunday, October 6, 2024 10:03:04 AM GMT+02:00 DST
301725 -> epoc 1728201790 -> Sunday, October 6, 2024 10:03:10 AM GMT+02:00 DST
301977 -> epoc 1728201780 -> Sunday, October 6, 2024 10:03:00 AM GMT+02:00 DST

### cleaner.py
This application sets the altitude to all elements in the csv file. The sensor provides always a set of 10 mesurements and only the last mesurement contains the altitude.