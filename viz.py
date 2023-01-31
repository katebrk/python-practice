from matplotlib import pyplot as plt
import pandas as pd


titanik_train = pd.read_csv("C:/Users/Ekaterina/Desktop/Preparation to work in UK/kaggle_titanik/train.csv")

titanik_train_sur = titanik_train.query("Survived == 1")
titanik_train_notsur = titanik_train.query("Survived == 0")

figure, axis = plt.subplots(1, 2)

axis[0].scatter(titanik_train_sur.SibSp, titanik_train_sur.Age, alpha = 0.1)
axis[0].set_title("Survived")

axis[1].scatter(titanik_train_notsur.SibSp, titanik_train_notsur.Age, label = "Not survived", alpha = 0.1)
axis[1].set_title("Not survived")

plt.show()
