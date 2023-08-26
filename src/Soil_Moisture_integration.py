import pandas as pd
from glob import glob
output_file_path = "/home/satyukt/Desktop/RAHUL/csv/CSV/s1/"
data_1 = pd.read_csv("/home/satyukt/Desktop/RAHUL/csv/s1_soil_moisture_madahalli.csv")
data_2 = pd.read_csv("/home/satyukt/Desktop/RAHUL/csv/sm_from_smap (1).csv")

output = pd.merge_ordered(data_1,data_2,
                 on= "Date", how= "outer")
print(output)
output_1 = output.drop("SMAP", axis = 1)
print(output_1)
output_2= output_1.drop('Singanallur',axis =1)
output_3 = output_2.rename(columns = {"K .Madahalli": "SMAP"})
print(output_3)                                               
output_4 = output_3.drop(output_3.index[0:17])                        
print(output_4)

# output_4.to_csv(f"{output_file_path}s1_soil_moisture_singanallur_.csv", index = False)
first_value_1 = output_4["S1 SM Mean"].dropna().iloc[0]
output_4["S1 SM Mean"] =  output_4["S1 SM Mean"].fillna(first_value_1)
# print(output_4)​
output_4["CF"] = output_4.apply(lambda x: (x["SMAP"]/x["S1 SM Mean"]),
                      axis = 1)
print(output_4)

output_4["Corrected_SM"] = output_4.apply(lambda x: (x["CF"]*x["S1 SM Pixel"]), axis = 1)
print(output_4)

for index, row in output_4.iterrows():
    if not pd.isnull(row['S1 SM Pixel']):
        # Use S1 SM Pixel if it's not NaN
        output_4.at[index, 'Corrected_SM'] = row[['S1 SM Pixel',"SMAP"]].max()
    elif not pd.isnull(row['SMAP']):
        # Use SMAP if S1 SM Pixel is NaN but SMAP is available
        prev_index = index - 1
        prev_s1_pixel = output_4.at[prev_index, 'S1 SM Pixel']
        current_smap = row['SMAP']
        diff_smap = current_smap - output_4.at[prev_index, 'SMAP']
        calculated_pixel = prev_s1_pixel + diff_smap
        output_4.at[index, 'S1 SM Pixel'] = calculated_pixel

print(output_4)

# Iterate over the dataframe rows
for index, row in output_4.iterrows():
    if pd.isnull(row['Corrected_SM']):
        # Fill NaN values in Corrected_SM using CF column * S1 SM Pixel
        cf_value = row['CF']
        s1_pixel_value = row['S1 SM Pixel']
        calculated_corrected_sm = cf_value * s1_pixel_value
        output_4.at[index, 'Corrected_SM'] = calculated_corrected_sm

# Print the updated dataframe
print(output_4)

output_4= output_4.drop(['S1 SM Pixel', 'S1 SM Mean', 'SMAP', 'CF'],axis =1)
print(output_4)
output_4.to_csv(f'{output_file_path}Soil_Moisture_ _____Madahalli.csv',index = False)
