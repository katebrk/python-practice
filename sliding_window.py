# 1. Maximim sum subarray of size k
# Given an array of positive numbers and a number k,
# find the maximum sum of any contiguous subarray of size k.

nums = [1, 5, 7, 3, 6, 9, 2]
k = 12

n = len(nums)
max_sum = 0

for i in range(n-k+1):
    nums_sum = nums[i]
    for j in range(i+1, i+k):
        nums_sum += nums[j]
    if nums_sum >= max_sum:
        max_sum = nums_sum

print(max_sum)

# 2. Smallest subarray with a given sum
# Given an array of positive numbers and a number S, find the length of the smallest contiguous subarray
# whose sum is greater than or equal to S. Return 0 if no such subarray exists.

nums = [1, 5, 7, 3, 6, 9, 2]
s = 12

n = len(nums)
min_len = float("inf")

for i in range(n):
    current_sum = 0
    for j in range(i, n):
        current_sum += nums[j]
        if current_sum >= s:
            min_len = min(min_len, j-i+1)
            break

print(min_len if min_len != float("inf") else 0)

