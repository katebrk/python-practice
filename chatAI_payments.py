import csv
import random
from datetime import datetime, timedelta

# Define constants
NUM_RECORDS = 50000
SUBSCRIPTION_IDS = list(range(1, 10001))
PRODUCT_IDS = [1, 2, 3, 4, 5]
COUNTRY_IDS = list(range(1, 11))

# Product distribution percentages
PRODUCT_DISTRIBUTION = {1: 0.43, 2: 0.17, 3: 0.15, 4: 0.10, 5: 0.15}
PRODUCT_AMOUNTS = {
    1: 0,
    2: (12, 25),
    3: (20, 30),
    4: (150, 250),
    5: 330
}

# Assign each subscription_id a fixed product_id and country_id
subscription_data = {
    sub_id: {
        "product_id": random.choices(PRODUCT_IDS, weights=[PRODUCT_DISTRIBUTION[p] for p in PRODUCT_IDS])[0],
        "country_id": random.choice(COUNTRY_IDS),
        "sub_start_date": None  # Will be assigned later
    }
    for sub_id in SUBSCRIPTION_IDS
}

# Generate weighted start dates (more subscriptions as time progresses)
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 5, 1)
dates = []
while len(dates) < len(SUBSCRIPTION_IDS):
    for month in range(18):
        count = (month + 1) * (len(SUBSCRIPTION_IDS) // 100)
        new_dates = [
            start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            for _ in range(count)
        ]
        dates.extend(new_dates)
random.shuffle(dates)
dates = dates[:len(SUBSCRIPTION_IDS)]  # Ensure it has exactly the right number of entries

# Assign sub_start_date to subscriptions
for i, sub_id in enumerate(SUBSCRIPTION_IDS):
    subscription_data[sub_id]["sub_start_date"] = dates[i]

# Adjust sub_start_date for product 4 and 5
for sub_id, data in subscription_data.items():
    if data["product_id"] in [4, 5] and data["sub_start_date"] < datetime(2024, 1, 10):
        data["sub_start_date"] = datetime(2024, 1, 10) + timedelta(
            days=random.randint(0, (end_date - datetime(2024, 1, 10)).days))


# Function to calculate next payment date
def get_next_payment_date(sub_start_date, last_payment_date):
    if last_payment_date is None:
        return sub_start_date + timedelta(days=30)
    next_date = last_payment_date + timedelta(days=30)
    return next_date if next_date <= datetime(2025, 5, 30) else None


# Generate payments
payments = []
last_payment_dates = {}

for i in range(NUM_RECORDS):
    subscription_id = random.choice(SUBSCRIPTION_IDS)
    product_id = subscription_data[subscription_id]["product_id"]
    country_id = subscription_data[subscription_id]["country_id"]
    sub_start_date = subscription_data[subscription_id]["sub_start_date"]

    # Ensure monthly payment_date for each subscription
    last_payment_date = last_payment_dates.get(subscription_id, None)
    payment_date = get_next_payment_date(sub_start_date, last_payment_date)

    if payment_date is None:
        continue  # Stop adding payments if the date exceeds the limit

    last_payment_dates[subscription_id] = payment_date

    # Set amount based on product_id
    if isinstance(PRODUCT_AMOUNTS[product_id], tuple):
        amount = round(random.uniform(*PRODUCT_AMOUNTS[product_id]), 2)
    else:
        amount = PRODUCT_AMOUNTS[product_id]

    payments.append([
        i + 1, subscription_id, product_id, country_id,
        sub_start_date.strftime('%Y-%m-%d'), '',
        amount, payment_date.strftime('%Y-%m-%d')
    ])

# Write to CSV
with open("payments.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([
        "payment_id", "subscription_id", "product_id", "country_id",
        "sub_start_date", "sub_end_date", "amount", "payment_date"
    ])
    writer.writerows(payments)

print("CSV file 'payments.csv' generated successfully.")