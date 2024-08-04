import csv
import random

def generate_person_data(num_people):
    # Predefined lists of concept IDs for gender, race, and ethnicity
    gender_concept_ids = [8507, 8532]  # 8507 = Male, 8532 = Female
    race_concept_ids = [8515, 8527, 8552]  # Example concept IDs for race
    ethnicity_concept_ids = [38003563, 38003564]  # Example concept IDs for ethnicity

    people_data = []

    for person_id in range(1, num_people + 1):
        person = {
            "person_id": person_id,
            "gender_concept_id": random.choice(gender_concept_ids),
            "year_of_birth": random.randint(1940, 2010),  # Example range for birth year
            "race_concept_id": random.choice(race_concept_ids),
            "ethnicity_concept_id": random.choice(ethnicity_concept_ids)
        }
        people_data.append(person)

    return people_data

def save_to_csv(data, filename):
    # Define the CSV column headers
    fieldnames = ["person_id", "gender_concept_id", "year_of_birth", "race_concept_id", "ethnicity_concept_id"]

    # Write data to CSV file
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for person in data:
            writer.writerow(person)

# Generate sample data for 5 people
sample_data = generate_person_data(5)

# Save the sample data to a CSV file
save_to_csv(sample_data, "sample_person_data.csv")

print("Sample data saved to sample_person_data.csv")
