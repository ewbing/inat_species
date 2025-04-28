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
    get_observations, 
    get_observation_species_counts,
    get_taxa,
    get_taxa_by_id
)

# Define rate limit: 60 calls per minute
CALLS = 60
RATE_LIMIT_PERIOD = 60  # seconds
PER_PAGE = 200  # Number of results per page    

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

def extract_taxonomy(taxon_dict):
    """
    Extract kingdom and phylum from taxon dictionary.
    Returns tuple of (kingdom, phylum)
    """
    kingdom = "Unknown"
    phylum = "Unknown"
    
    try:
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
        
        # Important phyla for algae and other common groups
        phyla_ids = {
            7683: "Rhodophyta",  # Red algae
            7639: "Chlorophyta",  # Green algae
            7629: "Ochrophyta",  # Brown algae, includes phaeophyceae
            47686: "Cyanobacteria",  # Blue-green algae
            20978: "Arthropoda",  # Arthropods
            3: "Chordata",  # Chordates
            11: "Mollusca",  # Mollusks
            21: "Annelida",  # Annelids
            5322: "Echinodermata",  # Echinoderms
            54: "Cnidaria",  # Cnidarians
            9611: "Porifera"  # Sponges
        }
        
        # First try to get from the ancestor_ids if available
        if 'ancestor_ids' in taxon_dict and taxon_dict['ancestor_ids']:
            for ancestor_id in taxon_dict['ancestor_ids']:
                if ancestor_id in kingdom_ids:
                    kingdom = kingdom_ids[ancestor_id]
                if ancestor_id in phyla_ids:
                    phylum = phyla_ids[ancestor_id]
        
        # Try direct properties if ancestry didn't work
        if kingdom == "Unknown" and 'kingdom_name' in taxon_dict and taxon_dict['kingdom_name']:
            kingdom = taxon_dict['kingdom_name']
        
        if phylum == "Unknown" and 'phylum_name' in taxon_dict and taxon_dict['phylum_name']:
            phylum = taxon_dict['phylum_name']
            
        # For red algae specific case
        if kingdom == "Unknown" and phylum == "Rhodophyta":
            kingdom = "Chromista"
            
        # For brown and green algae
        if kingdom == "Unknown" and phylum in ["Chlorophyta", "Ochrophyta"]:
            kingdom = "Chromista"
            
        return (kingdom, phylum)
        
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

def get_histogram_for_species(taxon_id, place_id, quality_grade="research", max_pages=5):
    """
    Fetch histogram data specifically for one species.
    Returns a 12-element list with counts for each month.
    """
    histogram = [0] * 12
    page = 1
    has_more = True
    
    print(f"Fetching individual observations for taxon {taxon_id}...")
    
    while has_more and page <= max_pages:
        try:
            observations = rate_limited_api_call(
                get_observations,
                taxon_id=taxon_id,
                place_id=place_id,
                quality_grade=quality_grade,
                per_page=PER_PAGE,
                page=page,
                returns="json"
            )
            
            # Process observations
            for obs in observations['results']:
                # Get observation month
                month = None
                if 'observed_on_details' in obs and obs['observed_on_details']:
                    date_obj = obs['observed_on_details'].get('date')
                    month = extract_month(date_obj)
                elif 'observed_on' in obs and obs['observed_on']:
                    month = extract_month(obs['observed_on'])
                
                # Update histogram if month was successfully extracted
                if month is not None:
                    histogram[month] += 1
            
            # Check if we have more pages
            total_results = observations.get('total_results', 0)
            has_more = total_results > page * PER_PAGE if total_results else False
            page += 1
            
        except Exception as e:
            print(f"Error fetching observations for taxon {taxon_id}: {e}")
            break
    
    return histogram

def fetch_detailed_taxon_info(taxon_id):
    """
    Fetch detailed taxonomy information for a specific taxon ID.
    Returns dictionary with detailed taxon information.
    """
    try:
        print(f"Fetching detailed taxonomy for taxon {taxon_id}...")
        taxon_response = rate_limited_api_call(
            get_taxa_by_id,
            taxon_id=taxon_id
        )
        
        if taxon_response and 'results' in taxon_response and taxon_response['results']:
            return taxon_response['results'][0]
        return {}
        
    except Exception as e:
        print(f"Error fetching detailed taxonomy for taxon {taxon_id}: {e}")
        return {}

def main():
    parser = argparse.ArgumentParser(description='Collect species data from FMR intertidal in iNaturalist')
    parser.add_argument('--output', default='fmr_intertidal_species.csv', help='Output CSV filename')
    parser.add_argument('--max_pages', type=int, default=10, help='Maximum number of pages to fetch')
    args = parser.parse_args()
    
    place_id = 51347  # FMR intertidal
    quality_grade = "research"  # Research grade observations only
    
    species_data = {}  # Store species data keyed by taxon_id
    
    print(f"Fetching species counts for FMR intertidal (place_id={place_id})...")
    
    # First get species counts to know what species exist in this location
    try:
        counts_response = rate_limited_api_call(
            get_observation_species_counts,
            place_id=place_id,
            quality_grade=quality_grade,
            # Limiting to make sure we don't exceed rate limits
            per_page=PER_PAGE
        )
        
        # Process species counts
        print(f"Found {len(counts_response['results'])} species. Processing...")
        
        # Create batch lists of taxon IDs to fetch detailed info
        all_taxon_ids = [result['taxon']['id'] for result in counts_response['results']]
        taxon_id_batches = [all_taxon_ids[i:i+100] for i in range(0, len(all_taxon_ids), 100)]
        
        # Store detailed taxon data
        taxon_details = {}
        
        # Fetch detailed taxon information in batches
        for batch in taxon_id_batches:
            print(f"Fetching detailed taxonomy for {len(batch)} taxa...")
            taxa_response = rate_limited_api_call(
                get_taxa,
                taxon_id=batch
            )
            
            # Store detailed taxon info
            for taxon in taxa_response['results']:
                taxon_details[taxon['id']] = taxon
        
        # Now process with detailed info
        unknown_kingdom_taxa = []
        
        for result in counts_response['results']:
            taxon_id = result['taxon']['id']
            count = result['count']
            common_name = result['taxon'].get('preferred_common_name', '')
            latin_name = result['taxon']['name']
            
            # Get detailed taxonomy if available
            taxon_dict = taxon_details.get(taxon_id, result['taxon'])
            
            # Extract kingdom and phylum
            kingdom, phylum = extract_taxonomy(taxon_dict)

            # Extract iconic taxon name
            iconic_taxon_name = taxon_dict.get('iconic_taxon_name', '')
            
            # Override iconic taxon name based on ancestor IDs
            if 'ancestor_ids' in taxon_dict and taxon_dict['ancestor_ids']:
                if 50863 in taxon_dict['ancestor_ids']:
                    iconic_taxon_name = "Green Algae"
                elif 57774 in taxon_dict['ancestor_ids']:
                    iconic_taxon_name = "Red Algae"
    
    
            # Keep track of taxa with unknown kingdom for further processing
            if kingdom == "Unknown":
                unknown_kingdom_taxa.append(taxon_id)
            
            # Store data about each species
            species_data[taxon_id] = {
                'kingdom': kingdom,
                'phylum': phylum,
                'common_name': common_name,
                'latin_name': latin_name,
                'taxon_id': taxon_id,
                'count': count,
                'histogram': [0] * 12,  # Initialize empty histogram for months
            }
        
        # For species with unknown kingdom, try to fetch individually
        if unknown_kingdom_taxa:
            print(f"Found {len(unknown_kingdom_taxa)} species with unknown kingdom. Fetching individually...")
            
            for taxon_id in unknown_kingdom_taxa:
                # Get more detailed taxonomy
                detailed_taxon = fetch_detailed_taxon_info(taxon_id)
                
                if detailed_taxon:
                    kingdom, phylum = extract_taxonomy(detailed_taxon)
                    species_data[taxon_id]['kingdom'] = kingdom
                    species_data[taxon_id]['phylum'] = phylum
                    
                    print(f"Updated taxonomy for {species_data[taxon_id]['latin_name']}: Kingdom={kingdom}, Phylum={phylum}")
    
    except Exception as e:
        print(f"Error fetching species counts: {e}")
        return
    
    # Now fetch observations to build histograms
    print(f"Fetching observations for {len(species_data)} species...")
    
    # Track species with all zeros in histogram
    zero_histogram_species = []
    
    # Start with bulk fetching for efficiency
    page = 1
    has_more = True
    
    print("Fetching observations in bulk...")
    
    # Using bulk requests to get observations
    while has_more and page <= args.max_pages:
        try:
            observations = rate_limited_api_call(
                get_observations,
                place_id=place_id,
                quality_grade=quality_grade,
                per_page=PER_PAGE,
                page=page,
                returns="json"
            )
            
            # Process observations to update histograms
            for obs in observations['results']:
                if 'taxon' in obs and obs['taxon'] and 'id' in obs['taxon']:
                    taxon_id = obs['taxon']['id']
                    # If this is a species we're tracking
                    if taxon_id in species_data:
                        # Get observation month
                        month = None
                        if 'observed_on_details' in obs and obs['observed_on_details']:
                            date_obj = obs['observed_on_details'].get('date')
                            month = extract_month(date_obj)
                        elif 'observed_on' in obs and obs['observed_on']:
                            month = extract_month(obs['observed_on'])
                        
                        # Update histogram if month was successfully extracted
                        if month is not None:
                            species_data[taxon_id]['histogram'][month] += 1
            
            # Check if we have more pages
            total_results = observations.get('total_results', 0)
            has_more = total_results > page * PER_PAGE if total_results else False
            page += 1
            
        except Exception as e:
            print(f"Error fetching bulk observations: {e}")
            page += 1
    
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
        fieldnames = ['kingdom', 'phylum', 'common_name', 'latin_name', 
                      'taxon_id', 'count', 'histogram', 'peak_month']
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by count (descending)
        sorted_species = sorted(species_data.values(), key=lambda x: x['count'], reverse=True)
        
        for species in sorted_species:
            writer.writerow({
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
