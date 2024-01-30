import pandas as pd
import numpy as np

df = pd.read_csv('/Users/pietro.calafiore/PycharmProjects/clinic-icp/fac_database.csv')

df.drop(["Record ID - Contact", "First Name", "Last Name", "Record ID - Company", "Company name"], axis=1,
        inplace=True)

# Define a function to assign 1 to all contacts whose value in the specific column is one of those

commercial_values = ['Self (Facility Online agenda)', 'Paid by facility (Facility Online agenda)',
                     'Paid by facility (Facility Premium)', 'Self (Facility Premium)']


def is_commercial_product(product):
    if isinstance(product, str):
        for value in commercial_values:
            if value in product:
                return 1
    return 0


df['Commercial'] = df['Doctor/Facility (Customer) - Products paid [MKPL Batch]'].apply(is_commercial_product)

# Swap a single value with 0 and change all unknown values with 0
df['Facility - number of doctors [DWH Batch]'] = df['Facility - number of doctors [DWH Batch]'].replace(10000000,
                                                                                                        float('nan'))
df['Facility - number of doctors [DWH Batch]'] = df['Facility - number of doctors [DWH Batch]'].fillna(0)
df['Facility - number of doctors SaaS only [DWH Batch]'] = df[
    'Facility - number of doctors SaaS only [DWH Batch]'].fillna(0)


# Create another column to store the average value for Facility - number of doctors [Forms], which are ranges

def compute_avg(range_val):
    if pd.notna(range_val):
        if '-' in range_val:
            values = range_val.split('-')
            numbers = [int(n) for n in values]
            avg = np.mean(numbers)
            return avg
        elif '+' in range_val:
            return 100
        else:
            return int(range_val)
    else:
        return np.nan


df['Number of Doc - Forms'] = df['Facility - number of doctors [Forms]'].apply(compute_avg)

df.drop(["Facility - number of doctors [Forms]"], axis=1,
        inplace=True)

# Create the ultimate 'number of doc' column, pulling data from the existing ones in order of reliability

conditions = [
    (df['Facility - number of doctors [DWH Batch]'].notna()),
    (df['Facility - number of doctors SaaS only [DWH Batch]'].notna()),
    (df['Number of Doc - Forms'].notna())
]

values = [
    df['Facility - number of doctors [DWH Batch]'],
    df['Facility - number of doctors SaaS only [DWH Batch]'],
    df['Number of Doc - Forms']
]

df['Doc Number'] = np.select(conditions, values, default=np.nan)

df.drop(["Facility - number of doctors [DWH Batch]", "Facility - number of doctors SaaS only [DWH Batch]",
         "Number of Doc - Forms"], axis=1,
        inplace=True)

# Create Spec columns: Dentist, Multi, None

df['Dentist'] = 0
df.loc[df['Facility - Specializations [DWH Batch]'].str.contains('dent', case=False, na=False), 'Dentist'] = 1

df['Multispecs'] = 0
df.loc[df['Facility - Specializations [DWH Batch]'].str.contains(',', case=False, na=False) & ~df[
    'Facility - Specializations [DWH Batch]'].str.contains(', ps', case=False, na=False), 'Multispecs'] = 1


df['Other Spec'] = (df['Dentist'].fillna(0) == 0) & (df['Multispecs'].fillna(0) == 0)

# Create Big City column

big_cities = ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza",
              "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre", "Roma", "Milano", "Napoli",
              "Torino", "Palermo", "Genova", "Bologna", "Firenze", "Bari", "Catania",
              "Madrid", "Barcelona", "Valencia", "Siviglia", "Zaragoza", "Malaga", "Murcia",
              "Palma", "Las Palmas de Gran Canaria", "Bilbao",
              "CDMX", "Guadalajara", "Monterrey", "Puebla", "Ciudad Juárez",
              "Tijuana", "León", "Zapopan", "Monclova", "Acapulco"]


def is_big_city(city):
    return 1 if city in big_cities else 0


df['Big City'] = df['City'].apply(is_big_city)

df.drop(["City"], axis=1,
        inplace=True)


# MQL column

def is_mql(last_source):
    if pd.notna(last_source) and last_source not in ['Target Tool [Outbound]', 'Sales Contact [Outbound]',
                                                     'Basic reference',
                                                     'New doctor profile verification', '[deleted marketing source]',
                                                     'Customer reference',
                                                     '[deleted customer source]', 'Massive assignment [Outbound]']:
        return 1
    return 0


df['MQL'] = df['Doctor/Facility - Last source [SO]'].apply(is_mql)


# Splitting columns' values into different columns

df = pd.get_dummies(df, columns=['CONTACT TYPE / SEGMENT'], prefix='SEGMENT', prefix_sep='_')
df = pd.get_dummies(df, columns=['Facility - Practice’s schedule management [Forms/manual]'], prefix='Management',
                    prefix_sep='_')
df = pd.get_dummies(df, columns=['Facility - Clinic Business Model [Upload]'], prefix='Business Model', prefix_sep='_')

# Datediff section

def compute_date_difference(df, start_col, end_col, result_col):
    df[start_col] = pd.to_datetime(df[start_col], errors='coerce')
    df[end_col] = pd.to_datetime(df[end_col], errors='coerce')

    condition = (df[start_col].notna()) & (df[end_col].notna())

    df[result_col] = (df[end_col] - df[start_col]).dt.days
    df.loc[~condition, result_col] = pd.NA
    df.loc[df[result_col] < 0, result_col] = np.nan


compute_date_difference(df, 'Create Date', 'Recent Deal Close Date', 'DD Create, Close Deal')
compute_date_difference(df, 'Doctor/Facility - Source [SO] at', 'Doctor/Facility - Recent deal created at (SO)',
                        'DD Source, Deal Created')
compute_date_difference(df, 'Doctor/Facility - Recent deal created at (SO)', 'Recent Deal Close Date',
                        'DD, Deal Create, Deal Close')


df['DD Create, Close Deal'] = df['DD Create, Close Deal'].fillna(0).astype(int)
df['DD Source, Deal Created'] = df['DD Source, Deal Created'].fillna(0).astype(int)
df['DD, Deal Create, Deal Close'] = df['DD, Deal Create, Deal Close'].fillna(0).astype(int)


# Final cleaning

df = df.loc[df['Country [MKPL Batch]'] == 'Mexico']


df.drop(['Doctor/Facility - Source [SO]', 'Doctor/Facility - Source [SO] at', 'Doctor/Facility - Last source [SO]',
         'Doctor/Facility - Last source [so] at', 'Facility - Specializations [DWH Batch]', 'Create Date',
         'Recent Deal Close Date', 'Doctor/Facility - Recent deal created at (SO)',
         'Doctor/Facility (Customer) - Products paid [MKPL Batch]', 'Country [MKPL Batch]'], axis=1, inplace=True)


df.fillna(0, inplace=True)
df = df.replace({True: 1, False: 0})


# Checking, Saving

for col in df.columns:
    print(col)

'''
for col in df.columns:
    print(col)
'''
row_number = df.shape[0]
print("Row number: ", row_number)

wip_clean = 'mx_fac.csv'
df.to_csv(wip_clean, index=False)
