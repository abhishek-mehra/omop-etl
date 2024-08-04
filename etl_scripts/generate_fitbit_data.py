import csv
import random
from datetime import datetime, timedelta
import pandas as pd

def load_person_ids_from_csv(filename):
    df = pd.read_csv(filename)
    return df['person_id'].tolist()

def generate_fitbit_data(person_ids, start_date, days, records_per_day):
    data = []
    types = ["activities-steps", "activities-heart", "activities-sleep", "activities-calories"]

    for person_id in person_ids:
        participant_identifier = random.randint(10000, 99999)
        current_datetime = start_date
        for _ in range(days * records_per_day):
            record_type = random.choice(types)
            record = {
                "ParticipantID": person_id,
                "ParticipantIdentifier": participant_identifier,
                "Type": record_type,
                "DateTime": current_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "Value": str(random.randint(0, 100)),
                "Level": str(random.randint(0, 3)),
                "Mets": str(round(random.uniform(5, 15), 3)),
                "Rmssd": str(round(random.uniform(15, 25), 3)),
                "Coverage": str(round(random.uniform(0.9, 1.1), 3)),
                "Hf": str(round(random.uniform(60, 80), 3)),
                "Lf": str(round(random.uniform(140, 170), 3)),
                "DeepSleepSummaryBreathRate": str(round(random.uniform(-1, 5), 1)),
                "RemSleepSummaryBreathRate": str(round(random.uniform(10, 12), 1)),
                "FullSleepSummaryBreathRate": str(round(random.uniform(13, 14), 1)),
                "LightSleepSummaryBreathRate": str(round(random.uniform(13, 14), 1)),
                "InsertedDate": "2022-08-01T00:00:00Z"
            }
            data.append(record)
            current_datetime += timedelta(minutes=1)
    
    return data

def save_fitbit_data_to_csv(data, filename):
    fieldnames = [
        "ParticipantID", "ParticipantIdentifier", "Type", "DateTime", "Value", "Level", 
        "Mets", "Rmssd", "Coverage", "Hf", "Lf", "DeepSleepSummaryBreathRate", 
        "RemSleepSummaryBreathRate", "FullSleepSummaryBreathRate", "LightSleepSummaryBreathRate", 
        "InsertedDate"
    ]
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in data:
            writer.writerow(record)

# Parameters
start_date = datetime(2021, 2, 10)
days = 1
records_per_day = 2  # 1440 minutes in a day

# Load person IDs from the person data CSV
person_ids = load_person_ids_from_csv("etl_scripts/sample_person_data.csv")

# Generate and save Fitbit data
fitbit_data = generate_fitbit_data(person_ids, start_date, days, records_per_day)
save_fitbit_data_to_csv(fitbit_data, "fitbit_dummy_data.csv")

print("Dummy Fitbit data saved to fitbit_dummy_data.csv")
