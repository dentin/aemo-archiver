### 0.10.1.1, 2016-04-07
* Updated the AEMO power station metadata.

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
