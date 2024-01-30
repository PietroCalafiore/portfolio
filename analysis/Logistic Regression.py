import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from tabulate import tabulate


df = pd.read_csv('/Users/pietro.calafiore/PycharmProjects/clinic-icp/mx_fac.csv')
'''
# First look at the dataframe
pd.set_option('display.width', 400)
pd.set_option('display.max_columns', None)
print(df.head())

# Any null values?
df.info()
print(df.describe())
sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis') # This command shows all the null values

plt.show()
'''
# Begin creating the model
# Build Predictors and Target

y = df["Commercial"] # Target
X = df.drop(["Commercial"], axis=1) # Predictors

# Scale them
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Create Train / Test subsets

X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.30, random_state=42)

# Work through logistic regression model

lr = LogisticRegression()
lr.fit(X_train, y_train)
y_pred = lr.predict(X_test)

# Let's test the accuracy

accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy:.2f}')

# Build the coefficients for all the variables: higher the coefficient is higher is the probability of 1

coefficients = lr.coef_[0]
feature_names = X.columns

feature_importance = pd.DataFrame({'Feature': feature_names, 'Coefficient': coefficients})
feature_importance = feature_importance.sort_values(by='Coefficient', ascending=False)
pd.set_option('display.max_colwidth', None)

# Show coefficient

print("Coefficient:")
# print(feature_importance)
print(feature_importance.to_string(index=False))

table = tabulate(feature_importance, headers='keys', tablefmt='pretty')
print(table)


# Show a plot with it

plt.figure(figsize=(12, 8))
sns.barplot(x='Coefficient', y='Feature', data=feature_importance)
plt.title('Most important coefficient', fontsize=16)
plt.xlabel('Coefficient', fontsize=14)
plt.ylabel('Feature', fontsize=14)
plt.xticks(fontsize=12)
plt.ylim(len(feature_importance) - 0.5, -0.5)
plt.tight_layout()
plt.savefig('coefficients_mx.png')
plt.show()
'''

'''