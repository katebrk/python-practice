def smallerNumbersThanCurrent(nums):
    counts = []
    for i in range(len(nums)):
        count = 0
        for j in range(len(nums)):
            if i != j and nums[j] < nums[i]:
                count += 1
        counts.append(count)
    return counts

# print(smallerNumbersThanCurrent([4, 6, 7]))



# функция печати матрицы
def printM(matrix):
    for row in matrix:
        print(row)


A = [[0, 3, 5],
     [5, 2, 0]]
printM(A)

B = [[3, 5, 3],
     [8, 45, 7]]
printM(B)

# сумма элементов в одной матрице
def sumM(matrix):
    sum = 0
    for row in matrix:
        for i in row:
            sum += i
    print(sum)

# сумма двух матриц
def sum2matrix(matrix1, matrix2):
    sum = []
    for y in range(len(matrix1)):
        row = []
        for x in range(len(matrix1[y])):
            z = matrix1[y][x] + matrix2[y][x]
            row.append(z)
        sum.append(row)
    return sum

printM(sum2matrix(A, B))

