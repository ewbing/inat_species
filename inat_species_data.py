#!/usr/bin/env python3
"""
A script to collect species data from iNaturalist for FMR intertidal (place_id=51347).
Outputs a CSV file with kingdom, phylum, common name, latin name, iNat ID, observation count,
histogram data, and month with most observations.
"""

import csv
import argparse
from datetime import datetime
from ratelimit import limits, sleep_and_retry
from pyinaturalist import (
    get_observation_species_counts,
    get_observation_histogram
    )

# Define rate limit: 60 calls per minute
CALLS = 60
RATE_LIMIT_PERIOD = 60  # seconds
PER_PAGE = 5  # Number of results per page    

@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT_PERIOD)
def rate_limited_api_call(func, **kwargs):
    """Execute an API call with rate limiting."""
    print(f"API call: {func.__name__} with params: {kwargs}")
    return func(**kwargs)

def get_month_with_most_obs(histogram):
    """Return the month with the most observations based on histogram data."""
    # Month numbers are 1-indexed (1=January)
    months = ["January", "February", "March", "April", "May", "June", 
              "July", "August", "September", "October", "November", "December"]
    
    # Find max month (histogram is 0-indexed)
    if not histogram or all(val == 0 for val in histogram):
        return "No data"
    
    max_month_index = histogram.index(max(histogram))
    return months[max_month_index]

def extract_taxonomy(ancestor_ids):
    """
    Extract kingdom and phylum from taxon dictionary.
    Returns tuple of (kingdom, phylum)
    """

    try:
        kingdom = "Unknown"
        phylum = "Unknown"

        # Common kingdom and phylum IDs in iNaturalist
        #  plant 47126, fungi 47170, animalia 1, protozoa 47686, chromista 48222, archaea 151817, bacteria 67333
        kingdom_ids = {
            1: "Animalia", 
            47126: "Plantae", 
            47170: "Fungi", 
            48222: "Chromista", 
            47686: "Protozoa", 
            67333: "Bacteria", 
            151817: "Archaea"
        }
        
        # Important phyla for algae and other common groups - TODO: bring in dynamically
        phyla_ids = {
            57774: "Rhodophyta",  # Red algae
            50863: "Chlorophyta",  # Green algae
            20978: "Arthropoda",  # Arthropods
            47115: "Mollusca",  # Mollusks
            47491: "Annelida",  # Annelids
            47549: "Echinodermata",  # Echinoderms
            47534: "Cnidaria",  # Cnidarians
        }
            
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

def extract_month(date_value):
    """Safely extract month from a date value that could be string or datetime."""
    try:
        # Handle datetime object
        if isinstance(date_value, datetime):
            return date_value.month - 1  # Convert to 0-indexed
        
        # Handle string in ISO format (YYYY-MM-DD)
        elif isinstance(date_value, str):
            return int(date_value.split('-')[1]) - 1  # Convert to 0-indexed
        
        return None
    except (IndexError, ValueError, AttributeError):
        return None

from pyinaturalist import get_observation_histogram

def get_histogram_for_species(taxon_id, place_id, quality_grade="research"):
    """
    Fetch histogram data for one species using get_observation_histogram.
    Returns a 12-element list with counts for each month.
    """
    try:
        params = {
            'taxon_id': taxon_id,
            'place_id': place_id,
            'quality_grade': quality_grade,
            'date_field': 'observed'
        }
        histogram_data = get_observation_histogram(**params)

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

def main():
    parser = argparse.ArgumentParser(description='Collect species data from FMR intertidal in iNaturalist')
    parser.add_argument('--output', default='inat_species_summary.csv', help='Output CSV filename')
    parser.add_argument('--max_pages', type=int, default=2, help='Maximum number of pages to fetch')
    args = parser.parse_args()
    
    place_id = 51347  # FMR intertidal
    quality_grade = "research"  # Research grade observations only
    
    species_results = []  # List to store species data from all pages
    page = 1  # Start from the first page

    species_data = {}  # Store species data keyed by taxon_id
    
    print(f"Fetching species counts for FMR intertidal (place_id={place_id})...")
    
    # First get species counts to know what species exist in this location

    # bulk fetching 
    page = 1    
    
    # Using bulk requests to get observations
    while True:
        counts_response = rate_limited_api_call(
            get_observation_species_counts,
            place_id=place_id,
            quality_grade=quality_grade,
            per_page=PER_PAGE
        )
        
        results = counts_response['results']
        if not results:
            print("No more results found.")
            break  # Exit if no results are returned

        species_results.extend(results)
        print(f"Fetched page {page} with {len(results)} species.")

        # Check if we have reached the max_pages limit
        if page >= args.max_pages:
            print(f"Reached the maximum page limit ({args.max_pages}).")
            break

        page += 1  # Move to the next page

    print(f"Total species fetched: {len(species_results)}.  Processing...")
        
    # Create batch lists of taxon IDs to fetch detailed info
    # all_taxon_ids = [result['taxon']['id'] for result in species_results]
    # taxon_id_batches = [all_taxon_ids[i:i+100] for i in range(0, len(all_taxon_ids), 100)]
    
    # Store detailed taxon data
    # taxon_details = {}
    
    # # Fetch detailed taxon information in batches
    # for batch in taxon_id_batches:
    #     print(f"Fetching detailed taxonomy for {len(batch)} taxa...")
    #     taxa_response = rate_limited_api_call(
    #         get_taxa,
    #         taxon_id=batch
    #     )
        
    #     # Store detailed taxon info
    #     for taxon in taxa_response['results']:
    #         taxon_details[taxon['id']] = taxon
    
    # Now process with detailed info
    unknown_kingdom_taxa = []
    
    for result in species_results:
        iconic_taxon_name = result['taxon']['iconic_taxon_name']
        taxon_id = result['taxon']['id']
        count = result['count']
        common_name = result['taxon'].get('preferred_common_name', '')
        latin_name = result['taxon']['name']

        # Get ancestor_ids
        ancestor_ids = result['taxon'].get('ancestor_ids', [])
                    
        kingdom, phylum = extract_taxonomy(ancestor_ids)    

        # Store data about each species
        species_data[taxon_id] = {
            'iconic_taxon_name': iconic_taxon_name,
            'kingdom': kingdom,
            'phylum': phylum,
            'common_name': common_name,
            'latin_name': latin_name,
            'taxon_id': taxon_id,
            'count': count,
            'histogram': [0] * 12,  # Initialize empty histogram for months
        }

        if kingdom == "Unknown":
            unknown_kingdom_taxa.append(taxon_id)

        
    # Now fetch observations to build histograms
    print(f"Fetching observations for {len(species_data)} species...")
    
    # Track species with all zeros in histogram (should be all of them)
    zero_histogram_species = []
        
    # Check which species have all zeros in histogram
    for taxon_id, data in species_data.items():
        if all(val == 0 for val in data['histogram']):
            zero_histogram_species.append(taxon_id)
    
    # For species with all zeros in histogram, try individual fetching
    if zero_histogram_species:
        print(f"Found {len(zero_histogram_species)} species with empty histograms. Fetching individually...")
        
        for taxon_id in zero_histogram_species:
            species_name = species_data[taxon_id]['latin_name']
            print(f"Fetching histogram for {species_name} (ID: {taxon_id})...")
            
            # Get histogram specifically for this species
            histogram = get_histogram_for_species(taxon_id, place_id, quality_grade)
            
            # Update histogram data
            species_data[taxon_id]['histogram'] = histogram
            
            # Check if we got any data
            if all(val == 0 for val in histogram):
                print(f"WARNING: Still no histogram data for {species_name} (ID: {taxon_id})")
            else:
                print(f"Successfully retrieved histogram for {species_name} (ID: {taxon_id})")
    
    # Calculate month with most observations for each species
    still_empty_histograms = []
    for taxon_id, data in species_data.items():
        data['peak_month'] = get_month_with_most_obs(data['histogram'])
        
        # Track species that still have no histogram data
        if data['peak_month'] == "No data":
            still_empty_histograms.append(f"{taxon_id} - {data['latin_name']} (K: {data['kingdom']}, P: {data['phylum']})")
    
    if still_empty_histograms:
        print(f"WARNING: {len(still_empty_histograms)} species still have no histogram data:")
        for species in still_empty_histograms:
            print(f"  - {species}")
    
    # Write data to CSV
    with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['iconic_taxon_name', 'kingdom', 'phylum', 'common_name', 'latin_name', 
                      'taxon_id', 'count', 'histogram', 'peak_month']
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by count (descending)
        sorted_species = sorted(species_data.values(), key=lambda x: x['count'], reverse=True)
        
        for species in sorted_species:
            writer.writerow({
                'iconic_taxon_name': species['iconic_taxon_name'],
                'kingdom': species['kingdom'],
                'phylum': species['phylum'],
                'common_name': species['common_name'],
                'latin_name': species['latin_name'],
                'taxon_id': species['taxon_id'],
                'count': species['count'],
                'histogram': species['histogram'],
                'peak_month': species['peak_month']
            })
    
    print(f"Data collection complete! Results saved to {args.output}")
    print(f"Total species: {len(species_data)}")

if __name__ == "__main__":
    main()
