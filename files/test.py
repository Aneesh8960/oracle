import pandas as pd

# Assuming df is your DataFrame
# Example DataFrame creation
data = {'Column1': [1, 2, 3],
        'Column2': ['A', 'B', 'C'],
        'Column3': [True, False, True]}
df = pd.DataFrame(data)

# Write DataFrame to a pipe delimited CSV file
df.to_csv('output_file.csv', sep='|', index=False)
