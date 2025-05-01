**iNaturalist Species Data Collector**
=====================================

**Overview**
------------

This script collects species data from iNaturalist for a specific place and outputs a CSV file with the following information:

* Kingdom
* Phylum
* Common name
* Latin name
* iNat ID
* Observation count
* Histogram data
* Month with most observations

It currently pulls from FMR intertidal (place_id=51347)

**Usage**
-----

1. Install required libraries: pip install -r requirements.txt
2. Run the script using `python inat_species_data.py`.
3. Optional arguments:
	* `--output`: specify the output CSV filename (default: `inat_species_summary.csv`)
	* `--place_id`: Place ID to fetch data for (default: 51347 - FMR intertidal)
	* `--filter`: Input CSV filename - used for filtering species IDs (default:species_filter.csv)

**Rate Limiting**
----------------
The script uses rate limiting to avoid exceeding iNaturalist's API call limits. The rate limit is set to 60 calls per minute.

**Output**
----------
The script outputs a CSV file with the collected species data. The file includes the following columns:
* `kingdom`
* `phylum`
* `common_name`
* `latin_name`
* `inat_id`
* `observation_count`
* `histogram_data`
* `month_with_most_observations`

**Notes**
-------
* The script uses the `pyinaturalist` library to interact with the iNaturalist API.
* The script may take several minutes to run depending on the number of pages fetched and the rate limit.
