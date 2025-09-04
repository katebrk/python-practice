# Find the greatest common divisor (GCD)

# Option 1

def find_divisors(nums):
    divisors = set()
    for i in range(1, nums+1):
        if nums % i == 0:
            divisors.add(i)
    return divisors

num1 = 1680
num2 = 640

divisors1 = find_divisors(num1)
divisors2 = find_divisors(num2)

# common divisors
common_divisors = sorted(divisors1 & divisors2)

# max common divisors
max_common_divisor = max(common_divisors)
print("Greatest common factor:", max_common_divisor)

# Option 2 - using Euclidean algorithm to find the GCD

num1 = 1680
num2 = 640

num_max = max(num1, num2)
num_min = min(num1, num2)

while num_max % num_min != 0:
    remainder = num_max % num_min
    num_max = num_min
    num_min = remainder
print("Greatest common factor:", num_min)