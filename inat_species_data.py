#!/usr/bin/env python3
"""
A script to collect species data from iNaturalist for a specific place.
Outputs a CSV file with kingdom, phylum, common name, latin name, iNat ID, observation count,
histogram data, and month with most observations.
"""

import csv
import argparse
from datetime import datetime
from ratelimit import limits, sleep_and_retry
from pyinaturalist import (
    get_observation_species_counts,
    get_observation_histogram,
    get_taxa,
)

# Define rate limit: e.g.30 calls per minute
CALLS = 30
RATE_LIMIT_PERIOD = 60  # seconds
PER_PAGE = 200  # Number of results per page
PAGE_SIZE = PER_PAGE  # Number of taxa to retrieve per page
MAX_PAGES = 5  # Maximum number of pages to fetch
DRY_RUN = False  # Set to True for testing without actual API calls


@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT_PERIOD)
def rate_limited_api_call(func, **kwargs):
    """
    Execute an API call with rate limiting.
    All pyinaturalist API calls should go through this function
    """
    print(f"API call: {func.__name__} with params: {kwargs}")
    return func(**kwargs)


def get_species_for_place(args, place_id, quality_grade):
    """
    Fetch species counts for a given place_id.

    Parameters
    ----------
    args : argparse.Namespace
        The parsed command line arguments.
    place_id : int
        The ID of the place to fetch species from.
    quality_grade : str
        The quality grade of the observations to fetch.

    Returns
    -------
    list
        A list of species data from all pages. Each element is a dictionary with the following keys:

            - taxon
            - count
    """

    print(f"Fetching species counts for place_id={place_id}...")
    species_results = []  # List to store species data from all pages

    page = 1  # Start from the first page

    # Using bulk requests to get observations
    while True:
        # Try to read the input CSV file to get the list of species IDs
        filter_ids = read_id_list_from_csv_file(args.filter)
        if filter_ids:
            print(
                f"Filtering species by IDs from {args.input}. Found {len(filter_ids)} species."
            )
            print(f"First 10 IDs from {args.input} are: {filter_ids[:10]}")

        counts_response = rate_limited_api_call(
            get_observation_species_counts,
            place_id=place_id,
            quality_grade=quality_grade,
            per_page=PER_PAGE,
            page=page,
            dry_run=DRY_RUN,
        )

        if filter_ids:
            # Filter the results based on the provided species IDs
            results = [
                result
                for result in counts_response["results"]
                if result["taxon"]["id"] in filter_ids
            ]
        else:
            results = counts_response["results"]

        if not counts_response:
            print("No more results found.")
            break  # Exit if no results are returned

        species_results.extend(results)
        print(f"Fetched page {page} with {len(results)} species.")

        # Check if we have reached the max_pages limit
        if page >= MAX_PAGES:
            print(f"Reached the maximum page limit ({MAX_PAGES}).")
            break

        page += 1  # Move to the next page

    print(f"Total species fetched: {len(species_results)}.  Processing...")
    return species_results


def fetch_phyla_ids():
    """
    Fetches phyla IDs with batch processing and
    assigns them to a global variable to limit API calls.

    Returns:
        dict: A dictionary mapping taxon IDs to their names.
    """
    try:
        for page in range(1, MAX_PAGES + 1):
            response = rate_limited_api_call(
                get_taxa, rank_level=60, per_page=PAGE_SIZE, page=page
            )
            results = response.get("results", [])
            if not results:
                print(f"No results found on page {page}.")
                break
            batch = {taxon["id"]: taxon["name"] for taxon in results}
            phyla_ids.update(batch)
            print(f"Fetched page {page} with {len(batch)} phyla.")
    except Exception as e:
        print(f"Error fetching phyla_ids: {e}")
    return phyla_ids


# Define phyla_ids globally to avoid repeated API calls
phyla_ids = {}


def extract_taxonomy(ancestor_ids):
    """
    Extract kingdom and phylum from taxon dictionary.
    Returns tuple of (kingdom, phylum)
    """

    try:
        kingdom = "Unknown"
        phylum = "Unknown"

        # Common kingdom and phylum IDs in iNaturalist
        kingdom_ids = {
            1: "Animalia",
            47126: "Plantae",
            47170: "Fungi",
            48222: "Chromista",
            47686: "Protozoa",
            67333: "Bacteria",
            151817: "Archaea",
        }

        # # Important phyla for algae and other common groups
        # phyla_ids = {
        #     2: 'Chordata',  # Chordates
        #     57774: "Rhodophyta",  # Red algae
        #     50863: "Chlorophyta",  # Green algae
        #     20978: "Arthropoda",  # Arthropods
        #     47115: "Mollusca",  # Mollusks
        #     47491: "Annelida",  # Annelids
        #     47549: "Echinodermata",  # Echinoderms
        #     47534: "Cnidaria",  # Cnidarians
        # }

        # Fetch phyla dyanmically (and lazily) if not already done
        if not phyla_ids:
            phyla_ids.update(fetch_phyla_ids())
            print(
                "Fetched phyla_ids - length is ",
                len(phyla_ids),
                " first 10 are:n",
                list(phyla_ids.items())[:10],
            )

        # The second element (index 1) should be the kingdom
        if len(ancestor_ids) > 1:
            kingdom_id = ancestor_ids[1]
            kingdom = kingdom_ids.get(kingdom_id, "Unknown")

        # The third element (index 2) should be the phylum
        if len(ancestor_ids) > 2:
            phylum_id = ancestor_ids[2]
            phylum = phyla_ids.get(phylum_id, "Unknown")

        # Fallback to direct properties if ancestry didn't work
        # if kingdom == "Unknown" and 'kingdom_name' in taxon_dict and taxon_dict['kingdom_name']:
        #     kingdom = taxon_dict['kingdom_name']

        # if phylum == "Unknown" and 'phylum_name' in taxon_dict and taxon_dict['phylum_name']:
        #     phylum = taxon_dict['phylum_name']

        return kingdom, phylum

    except Exception as e:
        print(f"Error extracting taxonomy: {e}")
        return ("Unknown", "Unknown")


def get_month_with_most_obs(histogram):
    """Return the month with the most observations based on histogram data."""
    # Month numbers are 1-indexed (1=January)
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    # Find max month (histogram is 0-indexed)
    if not histogram or all(val == 0 for val in histogram):
        return "No data"

    max_month_index = histogram.index(max(histogram))
    return months[max_month_index]


def extract_month(date_value):
    """Safely extract month from a date value that could be string or datetime."""
    try:
        # Handle datetime object
        if isinstance(date_value, datetime):
            return date_value.month - 1  # Convert to 0-indexed

        # Handle string in ISO format (YYYY-MM-DD)
        elif isinstance(date_value, str):
            return int(date_value.split("-")[1]) - 1  # Convert to 0-indexed

        return None
    except (IndexError, ValueError, AttributeError):
        return None


def get_histogram_for_species(taxon_id, place_id, quality_grade="research"):
    """
    Fetch histogram data for one species using get_observation_histogram.
    Returns a 12-element list with counts for each month.
    """
    try:
        params = {
            "taxon_id": taxon_id,
            "place_id": place_id,
            "quality_grade": quality_grade,
            "date_field": "observed",
            "dry_run": DRY_RUN,
        }
        histogram_data = rate_limited_api_call(get_observation_histogram, **params)

        # Initialize histogram with zeros for each month (0-indexed)
        histogram = [0] * 12
        for month_str, count in histogram_data.items():
            month = int(month_str) - 1  # Convert to 0-based index
            histogram[month] = count
        print("Histogram for taxon ", taxon_id, ":", histogram)

        return histogram
    except Exception as e:
        print(f"Error fetching histogram for taxon {taxon_id}: {e}")
        return [0] * 12


def get_histograms(place_id, quality_grade, species_data):
    """
    Fetch histogram data for each species in species_data.

    If some species have all zeros in their histogram, try fetching histogram
    data individually for those species.

    Returns the updated species_data dictionary with histogram data and
    month with most observations for each species.
    """
    print(f"Fetching histograms for {len(species_data)} species...")

    # Track species with all zeros in histogram (should be all of them)
    zero_histogram_species = []

    # Check which species have all zeros in histogram
    for taxon_id, data in species_data.items():
        if all(val == 0 for val in data["histogram"]):
            zero_histogram_species.append(taxon_id)

    # For species with all zeros in histogram, try individual fetching
    if zero_histogram_species:
        print(
            f"Found {len(zero_histogram_species)} species with empty histograms. Fetching..."
        )

        for taxon_id in zero_histogram_species:
            species_name = species_data[taxon_id]["latin_name"]
            print(f"Fetching histogram for {species_name} (ID: {taxon_id})...")

            # Get histogram specifically for this species
            histogram = get_histogram_for_species(
                taxon_id=taxon_id, place_id=place_id, quality_grade=quality_grade
            )

            # Update histogram data
            species_data[taxon_id]["histogram"] = histogram

            # Check if we got any data
            if all(val == 0 for val in histogram):
                print(
                    f"WARNING: Still no histogram data for {species_name} (ID: {taxon_id})"
                )
            else:
                print(
                    f"Successfully retrieved histogram for {species_name} (ID: {taxon_id})"
                )

    # Calculate month with most observations for each species
    still_empty_histograms = []
    for taxon_id, data in species_data.items():
        data["peak_month"] = get_month_with_most_obs(data["histogram"])

        # Track species that still have no histogram data
        if data["peak_month"] == "No data":
            still_empty_histograms.append(
                f"{taxon_id} - {data['latin_name']} (K: {data['kingdom']}, P: {data['phylum']})"
            )

    if still_empty_histograms:
        print(
            f"WARNING: {len(still_empty_histograms)} species still have no histogram data:"
        )
        for species in still_empty_histograms:
            print(f"  - {species}")


def read_id_list_from_csv_file(file_name):
    """
    Read a CSV file and return a list of integers. Each item in the file is
    converted to an integer, and if successful, added to the list. If not
    successful, the item is skipped. If the file is not found, an empty list is
    returned.

    Parameters
    ----------
    file_name : str
        The name of the file to read.

    Returns
    -------
    list
        A list of integers read from the file.
    """
    id_list = []  # List to store species IDs from the CSV file
    try:
        with open(file_name, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                for item in row:
                    try:
                        # Convert each item to an integer and add to the list
                        id_list.append(int(item))
                    except ValueError:
                        # Skip items that cannot be converted to integers
                        continue
            return id_list
    except FileNotFoundError:
        print(f"Taxon ID file {file_name} not found - will use all taxons")
        return []


def write_data_to_csv(data, filename):
    """
    Write species data to a CSV file.

    Parameters
    ----------
    data : dict
        A dictionary with species data, where each key is a taxon ID and the
        value is a dictionary with the following keys:
            - iconic_taxon_name
            - kingdom
            - phylum
            - common_name
            - latin_name
            - taxon_id
            - count
            - histogram
            - peak_month
    filename : str
        The name of the file to write the data to.

    Returns
    -------
    None

    Notes
    -----
    The file is written in the following format:

        iconic_taxon_name,kingdom,phylum,common_name,latin_name,taxon_id,count,histogram,peak_month

    The data is sorted by count in descending order.

    """
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "iconic_taxon_name",
            "kingdom",
            "phylum",
            "common_name",
            "latin_name",
            "taxon_id",
            "count",
            "histogram",
            "peak_month",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Sort by count (descending)
        sorted_data = sorted(data.values(), key=lambda x: x["count"], reverse=True)

        for item in sorted_data:
            writer.writerow(
                {
                    "iconic_taxon_name": item["iconic_taxon_name"],
                    "kingdom": item["kingdom"],
                    "phylum": item["phylum"],
                    "common_name": item["common_name"],
                    "latin_name": item["latin_name"],
                    "taxon_id": item["taxon_id"],
                    "count": item["count"],
                    "histogram": item["histogram"],
                    "peak_month": item["peak_month"],
                }
            )
        print(f"Data written to {filename}")


def main():
    """
    Main function to collect species data from iNaturalist for a specified place.

    This function parses command-line arguments to determine the output CSV
    filename, input filter CSV filename, and place ID for data collection.
    It fetches species data for the given place, processes the data to extract
    taxonomy information, and retrieves histogram data for each species. The
    processed data is then written to a CSV file.

    Command-line Arguments
    ----------------------
    --output : str, optional
        Output CSV filename (default: 'inat_species_summary.csv').
    --filter : str, optional
        Input CSV filename for filtering species IDs (default: 'species_filter.csv').
    --place_id : int, optional
        Place ID to fetch data for (default: 51347 - FMR intertidal).

    Outputs
    -------
    CSV file with species data including iconic taxon name, kingdom, phylum,
    common name, latin name, taxon ID, observation count, histogram data, and
    peak observation month.

    Notes
    -----
    - The function uses rate limiting to manage API calls.
    - The script collects only research grade observations.
    - Time taken for execution is printed at the end.
    """

    starttime = datetime.now()
    print("Start time:", starttime)

    parser = argparse.ArgumentParser(
        description="Collect species data from a place in iNaturalist"
    )
    parser.add_argument(
        "--output", default="inat_species_summary.csv", help="Output CSV filename"
    )
    parser.add_argument(
        "--filter",
        default="species_filter.csv",
        help="Input CSV filename - used for filtering species IDs",
    )
    parser.add_argument(
        "--place_id",
        type=int,
        default=51347,
        help="Place ID to fetch data for (default: 51347 - FMR intertidal)",
    )
    args = parser.parse_args()

    place_id = args.place_id
    quality_grade = "research"  # Research grade observations only

    species_results = get_species_for_place(args, place_id, quality_grade)

    species_data = {}  # Store species data keyed by taxon_id

    # Now process with detailed info
    unknown_kingdom_taxa = []

    for result in species_results:
        iconic_taxon_name = result["taxon"]["iconic_taxon_name"]
        taxon_id = result["taxon"]["id"]
        count = result["count"]
        common_name = result["taxon"].get("preferred_common_name", "")
        latin_name = result["taxon"]["name"]

        # Get ancestor_ids
        ancestor_ids = result["taxon"].get("ancestor_ids", [])

        kingdom, phylum = extract_taxonomy(ancestor_ids)

        # Store data about each species
        species_data[taxon_id] = {
            "iconic_taxon_name": iconic_taxon_name,
            "kingdom": kingdom,
            "phylum": phylum,
            "common_name": common_name,
            "latin_name": latin_name,
            "taxon_id": taxon_id,
            "count": count,
            "histogram": [0] * 12,  # Initialize empty histogram for months
        }

        if kingdom == "Unknown":
            unknown_kingdom_taxa.append(taxon_id)

    get_histograms(place_id, quality_grade, species_data)

    # Write data to CSV
    write_data_to_csv(species_data, args.output)

    print(f"Total species processed: {len(species_data)}")

    print(datetime.now())
    print("Total time taken:", datetime.now() - starttime)


if __name__ == "__main__":
    main()
