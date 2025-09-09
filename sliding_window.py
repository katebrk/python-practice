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

# 3. Find unique elements within window w in array s 
# Slide a window of size w across the array s and counting unique elements in each window

s = [1, 4, 5, 3, 2, 7, 3, 3, 6]
w = 3

n = len(s)
res = []

for i in range(n - w + 1):
    window_temp_res = []
    for j in range(i, i + w):
        window_temp_res.append(s[j])
    res.append(len(set(window_temp_res)))

print(res)

# output: [3, 3, 3, 3, 3, 2, 2]


# 4 / Leetcode 480. Sliding window median 
# You are given an integer array nums and an integer k. There is a sliding window of size k which is moving from the very left of the array to the very right. 
# You can only see the k numbers in the window. Each time the sliding window moves right by one position.
# Return the median array for each window in the original array. Answers within 10-5 of the actual value will be accepted.

nums = [1,3,-1,-3,5,3,6,7]
k = 3

n = len(nums)
res = []

for i in range(n - k + 1):  # i = 0
    temp_res = []
    for j in range(i, i + k):  # j = 0
        temp_res.append(nums[j])
    temp_res = sorted(temp_res)
    mid_ind = len(temp_res) // 2
    if len(temp_res) % 2 == 1:  # odd length
        res.append(temp_res[mid_ind])
    else:  # even lenght
        res.append((temp_res[mid_ind - 1] + temp_res[mid_ind]) / 2)

print(res)

# output: [1, -1, -1, 3, 5, 6]


# 5. Sliding window maximum 
# You are given an array of integers nums, there is a sliding window of size k 
# which is moving from the very left of the array to the very right. 
# You can only see the k numbers in the window. Each time the sliding window moves right by one position.
# Return the max sliding window.

nums = [1,3,-1,-3,5,3,6,7]
k = 3

n = len(nums)
res = []

for i in range(n - k + 1):
    temp_result = []
    for j in range(i, i + k):
        temp_result.append(nums[j])
    res.append(max(temp_result))
    
print(res)
# output: [3,3,5,5,6,7]