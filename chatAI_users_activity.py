import csv
import random
from datetime import datetime, timedelta

# Define constants
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 5, 30)
PRODUCT_IDS = [1, 2, 3, 4, 5]
COUNTRY_IDS = list(range(1, 11))

# Generate date range for calendar_month and rolling_30d
def generate_dates(start, end, frequency='daily'):
    current = start
    dates = []
    while current <= end:
        dates.append(current)
        if frequency == 'monthly':
            next_month = current.month % 12 + 1
            next_year = current.year + (1 if next_month == 1 else 0)
            current = current.replace(year=next_year, month=next_month, day=1)
        else:
            current += timedelta(days=1)
    return dates

calendar_month_dates = generate_dates(START_DATE, END_DATE, 'monthly')
rolling_30d_dates = generate_dates(START_DATE, END_DATE, 'daily')

# Generate user activity data
def generate_activity_data(date_list, activity_type):
    activity_data = []
    for date in date_list:
        for product_id in PRODUCT_IDS:
            for country_id in COUNTRY_IDS:
                active_users = random.randint(100, 10000)
                new_users = random.randint(int(0.1 * active_users), int(0.6 * active_users))
                churned_users = random.randint(int(0.02 * active_users), int(0.1 * active_users))
                activity_data.append([
                    date.strftime('%Y-%m-%d'), activity_type, product_id, country_id,
                    active_users, new_users, churned_users
                ])
    return activity_data

calendar_month_data = generate_activity_data(calendar_month_dates, 'calendar_month')
rolling_30d_data = generate_activity_data(rolling_30d_dates, 'rolling_30d')

# Combine data
all_activity_data = calendar_month_data + rolling_30d_data

# Write to CSV
with open("users_activity.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["date", "type", "product_id", "country_id", "active_users", "new_users", "churned_users"])
    writer.writerows(all_activity_data)

print("CSV file 'users_activity.csv' generated successfully.")