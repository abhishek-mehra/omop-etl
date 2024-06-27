import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_fitbit_data(num_days=30, num_users=10):
    start_date = datetime.now().date() - timedelta(days=num_days)
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    user_ids = [f"user_{i}" for i in range(1, num_users+1)]

    data = []
    for user_id in user_ids:
        for date in dates:
            steps = np.random.randint(1000, 20000)
            heart_rate = np.random.randint(60, 100)
            sleep_duration = np.random.randint(5, 10)
            calories = np.random.randint(1500, 3000)
            
            data.append({
                'user_id': user_id,
                'date': date,
                'steps': steps,
                'avg_heart_rate': heart_rate,
                'sleep_duration': sleep_duration,
                'calories_burned': calories
            })

    df = pd.DataFrame(data)
    return df

# Generate data
fitbit_data = generate_fitbit_data()

# Save to CSV
fitbit_data.to_csv('fitbit_data.csv', index=False)
print("Dummy Fitbit data saved to fitbit_data.csv")