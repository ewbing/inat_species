from pyinaturalist import (
    get_observation_species_counts,
    get_taxa,
    get_taxa_by_id,
    get_observations,
    get_observation_histogram,
)

PLACE_ID = 51347
QUALITY_GRADE = "research"
PER_PAGE = 10
TAXON_ID = 117775


def get_observation_species_counts_call():
    params = {
        "place_id": PLACE_ID,
        "quality_grade": QUALITY_GRADE,
        "per_page": PER_PAGE,
        "taxon_id": TAXON_ID,
    }
    response = get_observation_species_counts(**params)
    print("Call to get_observation_species_counts with place_id=51347")
    print(response)


def get_taxon_dict_for_observation():
    params = {
        "place_id": PLACE_ID,
        "quality_grade": QUALITY_GRADE,
        "per_page": PER_PAGE,
        "taxon_id": TAXON_ID,
    }
    print("Call to get_observation_species_counts with place_id={PLACE_ID}")
    response = get_observation_species_counts(**params)
    taxon_dict = response["results"][0]["taxon"]["ancestor_ids"]
    print(taxon_dict)


def get_taxa_by_id_call():
    params = {"taxon_id": TAXON_ID}
    response = get_taxa_by_id(**params)
    print("\n\nCall to get_taxa_by_id with taxon_id=117775")
    print(response)


def get_observations_call():
    params = {
        "taxon_id": 5206,
        "place_id": 51347,
        "quality_grade": "research",
        "per_page": 1,
        "page": 1,
        "returns": "json",
    }
    response = get_observations(**params)
    print("\n\nCall to get_observations with taxon_id=5206")
    print(response)


def get_observation_histogram_call():
    params = {
        "place_id": PLACE_ID,
        "quality_grade": QUALITY_GRADE,
        "per_page": PER_PAGE,
        "taxon_id": TAXON_ID,
    }
    print("\n\nCall to get_observation_histogram with taxon_id={TAXON_ID}")
    histogram_data = get_observation_histogram(**params)
    print(histogram_data)

    # Initialize histogram with zeros for each month (0-indexed)
    histogram = [0] * 12
    for month_str, count in histogram_data.items():
        month = int(month_str) - 1  # Convert to 0-based index
        histogram[month] = count
    print("Histogram:", histogram)


def get_phyla_taxa():
    params = {
        "rank_level": 60,  # Phylum rank level
        "per_page": 100,  # Number of taxa to retrieve per page
    }
    phyla_response = get_taxa(**params)
    phyla_ids = {taxon["id"]: taxon["name"] for taxon in phyla_response["results"]}
    print("Number of phyla:", len(phyla_ids))
    print(phyla_ids)


def main():
    #    get_observation_species_counts_call()
    #    get_taxa_by_id_call()
    #    get_taxon_dict_for_observation()
    #    get_observations_call()
    #    get_observation_histogram_call()
    get_phyla_taxa()


if __name__ == "__main__":
    main()
