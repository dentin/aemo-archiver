AEMO-archiver
=============

Fetches 5 minute AEMO power station data for insertion into database


Update AEMO metadata
====================

To update the AEMO metadata run the `./registrations` script. This depends on `ssconvert` (part of Gnumeric), `xsltproc` and a Java JDK in order to run. Once you get it to run successfully compare the differences of the `power_station_metadata/power_station_locations.csv` file, and if there are differences that look suspect then investigate. The other changed files may shed more light on why a difference has shown up. Also note that it appears there are differences between the output of GNU `sort` and the OSX BSD `sort` commands. I recommend always running on OSX.
