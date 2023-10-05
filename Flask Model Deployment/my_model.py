# Import libraries
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# Load model training data
data = pd.read_csv("C:/Users/ethan/OneDrive/Documents/Data Glacier/datasets/hr_dashboard_data.csv")
data = data.drop(['Name', 'Department', 'Projects Completed', 'Position', 'Joining Date'], axis=1)

# Assign variables
X = data.iloc[:, :5]
y = data.iloc[:, -1]

# Fit a linear regression model with training data
LR = LinearRegression()
LR.fit(X,y)

# Save model to disk
pickle.dump(LR, open('my_model.pkl', 'wb'))
my_model = pickle.load(open('my_model.pkl', 'rb'))

print(my_model.predict([[20,1,50,50,4]]))