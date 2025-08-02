Final Pipeline Using Your Month-Based Definitions
Assuming you have this after aggregation:

python
Copy
Edit
# Example of your daily dataframe
# daily_calls:
# | date       | call_volume |
# |------------|-------------|
# | 2022-04-01 | 1050        |

daily_calls['date'] = pd.to_datetime(daily_calls['date'])
🔹 Step 1: Add month and period_type
python
Copy
Edit
# Extract full month name
daily_calls['month_name'] = daily_calls['date'].dt.strftime('%B')

# Assign "holiday", "school", or "other"
def classify_period(month):
    if month in holiday_months:
        return "holiday"
    elif month in school_months:
        return "school"
    else:
        return "other"

daily_calls['period_type'] = daily_calls['month_name'].apply(classify_period)
Now you’ll have:

date	call_volume	month_name	period_type
2022-04-01	1050	April	holiday
2022-06-01	1150	June	school
2022-01-01	980	January	other

🔹 Step 2: Visualize
Boxplot: School vs Holiday
python
Copy
Edit
import seaborn as sns
import matplotlib.pyplot as plt

sns.boxplot(x='period_type', y='call_volume', data=daily_calls[daily_calls['period_type'] != 'other'])
plt.title("Call Volume: Holiday vs School Months")
plt.show()
🔹 Step 3: Statistical Test: Holiday vs School
python
Copy
Edit
from scipy.stats import mannwhitneyu

holiday_calls = daily_calls[daily_calls['period_type'] == 'holiday']['call_volume']
school_calls = daily_calls[daily_calls['period_type'] == 'school']['call_volume']

stat, p_value = mannwhitneyu(holiday_calls, school_calls)
print(f"Mann-Whitney U Test p-value: {p_value:.4f}")
A p-value < 0.05 means the difference in call volume between holiday and school months is statistically significant.

🔹 Step 4: Optional — Mean & Standard Deviation Summary
python
Copy
Edit
daily_calls.groupby('period_type')['call_volume'].agg(['mean', 'std', 'count'])
🔹 Step 5: Optional — Line Plot to See Patterns Over the Year
python
Copy
Edit
daily_calls['day_of_year'] = daily_calls['date'].dt.dayofyear

# Average across all years for smoothing
avg_by_day = daily_calls.groupby('day_of_year')['call_volume'].mean()

plt.figure(figsize=(14, 4))
avg_by_day.plot()
plt.title("Average Daily Call Volume by Day of Year (All Years)")
plt.xlabel("Day of Year")
plt.ylabel("Avg Call Volume")
plt.grid()
plt.show()

#---------------------------------
