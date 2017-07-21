## Next
* Update power station metadata.

## 0.12.2.0, 2017-04-21
* Actually fix previous bug...

## 0.12.1.0, 2017-04-19
* Fix bug where CSV's contained extraneous spaces in DUIDs

## 0.12.0.0, 2017-04-18
* Strip spaces from PowerStation datatype when inserted into database (Part of the fix for #4)
* Update to lts-8.11 (and GHC 8.0.2 to fix build error on macOS with GHC < 8.0.2)

### 0.11.0.0, 2016-07-29
* Switch to using Wreq for all HTTP requests, should be a bit faster and nicer on the server.

### 0.10.1.2, 2016-07-14
* Partition table SQL script added.

### 0.10.1.1, 2016-04-07
* Updated the AEMO power station metadata.
* Use new configuration format, see config/aemo-archiver.yaml and the `--help` flag in executables

### 0.8.2.0, 2016-02-24
* Updated the AEMO power station metadata.

### 0.8.1.1, 2015-11-23
* Fix `make_package` to include the `sync-latest` tool properly.

### 0.8.1.0, 2015-11-23
* Change materialised view to be a manually updated table named `latest_power_station_datum`.
* Add the tool `sync-latest` to update the `latest_power_station_datum`.

### 0.8.0.1, 2015-10-16
* Update to the power station metadata.

### 0.8.0.0, 2015-09-25
* Create a "materialized view" of the most recent time a DUID has been seen.

### 0.7.0.1, 2015-09-22
 * Ensure that all data in a zip file is inserted in a single transaction (including archives)

  Other:
   - Fix bug where timezone information wasn't being added to timestamps, version bump
   - Make check for existing zip files a little more efficient
   - Do fail if we see a ZipFile in a ZipTree when inserting CSVs - they should not exist
   - Version bump
   - Remove Data.Functor import, add AEMO.ZipTree to cabal file
   - make AEMO.ZipTree stylish
   - Use ZipTree to process zip files
   - Add AEMO.ZipTree which handles archives and flat zip files as a tree

  Contributors:
   - Alex Mason


### 0.6.0.0, 2015-07-27

* Check each available file against the database instead of loading all know files from the database and checking in memory.
* Refactor to use Stack/Stackage.
