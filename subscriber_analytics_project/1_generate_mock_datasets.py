import pandas as pd
import numpy as np

# Constants
n_subscribers = 1000
date_range = pd.date_range(start="2024-01-01", end="2024-12-31", freq="D")
school_holidays = pd.to_datetime([
    "2024-04-08", "2024-04-09", "2024-08-12", "2024-08-13", "2024-12-02", "2024-12-03"
])

def simulate_data(source_name):
    np.random.seed(42 if source_name == "set_a" else 99)
    data = []
    for nr_sbsc in range(1, n_subscribers + 1):
        for date in date_range:
            is_holiday = date in school_holidays
            call_count = np.random.poisson(2 if not is_holiday else 3)
            msg_count = np.random.poisson(1.5 if not is_holiday else 2)
            data_mb = np.random.normal(loc=100 if not is_holiday else 120, scale=20)
            data.append({
                "nr_sbsc": f"SBSC{nr_sbsc:04d}",
                "date": date,
                "call_count": max(call_count, 0),
                "msg_count": max(msg_count, 0),
                "data_mb": round(max(data_mb, 0), 2),
                "source": source_name
            })
    return pd.DataFrame(data)

df_a = simulate_data("set_a")
df_b = simulate_data("set_b")

df_a.to_csv("dataset_a.csv", index=False)
df_b.to_csv("dataset_b.csv", index=False)
pd.Series(school_holidays).to_csv("kenya_school_holidays.csv", index=False, header=["date"])
