# One or more of the following lines contains an error
# Correct it so that it runs without producing syntax errors

import pandas as pd
import sys

# import csv file
titanik_test = pd.read_csv("C:/Users/Ekaterina/Desktop/Preparation to work in UK/kaggle_titanik/test.csv", index_col=0)
titanik_train = pd.read_csv("C:/Users/Ekaterina/Desktop/Preparation to work in UK/kaggle_titanik/train.csv",
                            index_col=0)


# step 1, вывод значений с 892 по 1309
# f = open('out.csv', 'w')
# sys.stdout = f

# step 2 we check sex
# print('PassengerId,Survived')
# for id in range(892, 1310):
#     if titanik.Sex[id] == "female":
#         survived = 1
#     else:
#         survived = 0
#     print(str(id) + "," + str(survived))

# step 3 we check sex, age
# print('PassengerId,Survived')
# for id in range(892, 1310):
#     if titanik_test.Sex[id] == "female" or titanik_test.Age[id] <= 16:
#         survived = 1
#     else:
#         survived = 0
#     print(str(id) + "," + str(survived))

# step 4 check train data
# наш текущий алгоритм (модель)
def hyp_is_survived(ds, f_index):
    f_survived = 0
    if ds.Sex[f_index] == "female" or ds.Age[f_index] <= 12:
        f_survived = 1
    if ds.Pclass[f_index] == 3 and ds.Age[f_index] < 50:
        f_survived = 0
    return f_survived


def print_test():
    f = open('out.csv', 'w')
    sys.stdout = f
    print('PassengerId,Survived')
    for x in titanik_test.index:
        survived = hyp_is_survived(titanik_test, x)
        print(str(x) + "," + str(survived))


def print_train():
    count_right = 0
    count_wrong = 0
    for x in titanik_train.index:
        survived = hyp_is_survived(titanik_train, x)
        if survived == titanik_train.Survived[x]:
            count_right = count_right + 1
        else:
            count_wrong = count_wrong + 1
    print("correct: " + str(count_right))
    print("wrong: " + str(count_wrong))
    print(count_right / (count_right + count_wrong))


print_train()
print_test()


