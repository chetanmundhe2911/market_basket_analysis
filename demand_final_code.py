import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import shapiro, ttest_ind, mannwhitneyu
import warnings
warnings.filterwarnings("ignore")

# -----------------------
# 📌 Configurable Input
# ---------------------

# Replace this with your actual daily call volume DataFrame
# Make sure it has: df['date'], df['call_volume']
df_daily = pd.read_csv("your_daily_calls.csv", parse_dates=['date'])

# Define month groups
holiday_months = {"April", "August", "November", "December"}
school_months = {"May", "June", "September", "October"}

# ---------------------
# 🔹 Step 1: Classify Period
# ---------------------
df_daily['month'] = df_daily['date'].dt.strftime('%B')

def classify_period(month):
    if month in holiday_months:
        return 'holiday'
    elif month in school_months:
        return 'school'
    else:
        return 'other'

df_daily['period_type'] = df_daily['month'].apply(classify_period)

# ---------------------
# 🔹 Step 2: Split Groups
# ---------------------
school = df_daily[df_daily['period_type'] == 'school']['call_volume']
holiday = df_daily[df_daily['period_type'] == 'holiday']['call_volume']

# ---------------------
# 🔹 Step 3: Check Normality
# ---------------------
school_normal = shapiro(school.sample(n=min(len(school), 5000)))[1] > 0.05
holiday_normal = shapiro(holiday.sample(n=min(len(holiday), 5000)))[1] > 0.05

print(f"✅ School Normal? {school_normal}")
print(f"✅ Holiday Normal? {holiday_normal}")

# ---------------------
# 🔹 Step 4: Choose Test & Run
# ---------------------
if school_normal and holiday_normal:
    test_name = "Welch's t-test"
    stat, p = ttest_ind(school, holiday, equal_var=False)
else:
    test_name = "Mann-Whitney U test"
    stat, p = mannwhitneyu(school, holiday)

# ---------------------
# 🔹 Step 5: Compute Effect Size
# ---------------------
mean_diff = abs(school.mean() - holiday.mean())
pooled_std = np.sqrt((school.std()**2 + holiday.std()**2) / 2)
cohens_d = mean_diff / pooled_std

# ---------------------
# 🔹 Step 6: Print Summary
# ---------------------
print("\n🔍 Test Summary")
print(f"Test Used      : {test_name}")
print(f"p-value        : {p:.4f}")
print(f"Mean (School)  : {school.mean():.2f}")
print(f"Mean (Holiday) : {holiday.mean():.2f}")
print(f"Cohen's d      : {cohens_d:.2f}")

# ------------------------
# 🔹 Step 7: Visualization
# ---------------------
sns.boxplot(x='period_type', y='call_volume', data=df_daily[df_daily['period_type'].isin(['school', 'holiday'])])
plt.title("Daily Call Volume: School vs Holiday Periods")
plt.grid(True)
plt.show()

#---------------------------------------==========

