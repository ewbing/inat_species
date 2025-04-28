from pyinaturalist import get_observation_species_counts,get_taxa_by_id

params = {
    'place_id': 51347,
    'quality_grade': 'research',
    'per_page': 10,
    'taxon_id': 117775   # Green pin-cushion alga	Cladophora columbiana
}

response = get_observation_species_counts(**params)

# format json at https://jsonviewer.stack.hu/
print("Call to get_observation_species_counts with place_id=51347")
print(response)


"""
Fetch detailed taxonomy information for a specific taxon ID.
Returns dictionary with detailed taxon information.
"""

params = {
    'taxon_id': 117775   # Green pin-cushion alga	Cladophora columbiana
}

response =  get_taxa_by_id(**params)
print("\n\nCall to get_taxa_by_id with taxon_id=117775")
print(response)



