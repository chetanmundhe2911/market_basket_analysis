Load and preprocess:

python
Copy
Edit
import pandas as pd

# Load your dataset
df = pd.read_csv('call_volume_daily.csv')
df['date'] = pd.to_datetime(df['date'])

# Ensure data is sorted
df = df.sort_values('date')
🔹 2. Add Holiday Indicators (School + Public)
(A) Kenyan Public Holidays
python
Copy
Edit
kenyan_public_holidays = [
    "2022-01-01", "2022-12-25", "2023-01-01", "2023-12-25",
    "2024-01-01", "2024-12-25", "2025-01-01", "2025-12-25",
    # Add Eid, Mashujaa Day, Madaraka Day, etc. from official list
]
df['is_public_holiday'] = df['date'].isin(pd.to_datetime(kenyan_public_holidays))
(B) Kenyan School Holidays
Kenya typically has 3 term breaks — around April, August, and December:

python
Copy
Edit
school_holidays = [
    # 2022
    ("2022-04-01", "2022-04-30"),
    ("2022-08-01", "2022-08-30"),
    ("2022-12-01", "2022-12-31"),
    # 2023
    ("2023-04-01", "2023-04-30"),
    ("2023-08-01", "2023-08-30"),
    ("2023-12-01", "2023-12-31"),
    # Continue for 2024–2025...
]

def is_school_holiday(date):
    return any(pd.to_datetime(start) <= date <= pd.to_datetime(end) for start, end in school_holidays)

df['is_school_holiday'] = df['date'].apply(is_school_holiday)
🔹 3. Exploratory Analysis
Boxplots
python
Copy
Edit
import seaborn as sns
import matplotlib.pyplot as plt

sns.boxplot(x='is_school_holiday', y='call_volume', data=df)
plt.title("Call Volume: School Holiday vs School Days")
plt.show()

sns.boxplot(x='is_public_holiday', y='call_volume', data=df)
plt.title("Call Volume: Public Holiday vs Non-Holiday")
plt.show()
Line Plot Over Time (for trend understanding)
python
Copy
Edit
df.set_index('date')['call_volume'].plot(figsize=(15,4), title='Daily Call Volume Over Time')
plt.show()
🔹 4. Statistical Testing
School Holiday vs School Days
python
Copy
Edit
from scipy.stats import ttest_ind, mannwhitneyu

calls_school = df[df['is_school_holiday'] == True]['call_volume']
calls_regular = df[df['is_school_holiday'] == False]['call_volume']

# Use Mann-Whitney U Test (non-parametric)
u_stat, p_val = mannwhitneyu(calls_school, calls_regular)
print("School holiday p-value:", p_val)
Public Holiday vs Regular Days
python
Copy
Edit
calls_public = df[df['is_public_holiday'] == True]['call_volume']
calls_non_public = df[df['is_public_holiday'] == False]['call_volume']

u_stat, p_val = mannwhitneyu(calls_public, calls_non_public)
print("Public holiday p-value:", p_val)
🔹 5. Optional: Linear Regression (Effect Size Estimation)
python
Copy
Edit
import statsmodels.api as sm

df['is_school_holiday'] = df['is_school_holiday'].astype(int)
df['is_public_holiday'] = df['is_public_holiday'].astype(int)

X = df[['is_school_holiday', 'is_public_holiday']]
X = sm.add_constant(X)
y = df['call_volume']

model = sm.OLS(y, X).fit()
print(model.summary())
This will show whether holidays reduce or increase demand and by how much, on average.

🔹 6. Result Summary: What to Report
You can summarize:

Factor	Holiday Type	Mean Call Volume	Effect (Δ)	p-value	Conclusion
School Holidays	True	1234	↓ ~100	0.01	Significant drop in demand
Public Holidays	True	1450	No change	0.45	Not significant


#----------------
