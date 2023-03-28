# импорт модулей для линейной регрессии
import numpy as np
from sklearn.linear_model import LinearRegression

# создание массива x (входов), для того чтобы массив был двумерным делаем reshape. создание массива у (выходов)
x = np.array([5, 15, 25, 35, 45, 55]).reshape((-1, 1)) # features
y = np.array([5, 20, 14, 32, 22, 38]) # prediction target
print(x)
print(y)

# создание переменной в качестве экземпляра LinearRegression
# model = LinearRegression()
# вызов переменной model, fit вычисляет оптимальное значение весов используя x и y в кач-ве аргумента
# model.fit(x, y)

# создание переменной в кач-ве LinearRegression, вычисление оптимального значения весов
model = LinearRegression().fit(x, y)

# просмотр атрибутов b0, b1
print('intercept:', model.intercept_)
print('slope:', model.coef_)

# проверка удовлетворительности рез-ов
r_sq = model.score(x, y)
print('coefficient of determination:', r_sq)


# прогноз, одномерный массив
y_pred = model.predict(x)
print('predicted response:', y_pred, sep='\n')

# почти идентичный способ предсказать ответ, только получаем двумерный массив
y_pred = model.intercept_ + model.coef_ * x
print('predicted response:', y_pred, sep='\n')




# при y - двумерный массив
new_model = LinearRegression().fit(x, y.reshape((-1, 1)))
print('intercept:', new_model.intercept_)
print('slope:', new_model.coef_)



