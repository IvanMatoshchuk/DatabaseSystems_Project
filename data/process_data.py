import sys
import os

import pandas as pd
import numpy as np
from functools import reduce

from pathlib import Path

from sqlalchemy import create_engine


def load_data_and_preprocess():
    '''
    function for data loading
    
    input: filepaths for "messages" and "categories" datasets
    '''


    path_to_data = Path(__file__).parent
    #path_to_data = os.path.join(path_to_data, "")
 
    # read data
    data_co2 = pd.read_csv(os.path.join(path_to_data, "co2_emission.csv"))
    data_gdp = pd.read_csv(os.path.join(path_to_data, "gdp.csv"))
    data_pop_grow = pd.read_csv(os.path.join(path_to_data, "population_growth.csv"))
    data_pop_total = pd.read_csv(os.path.join(path_to_data, "population_total.csv"))
    data_export = pd.read_excel(os.path.join(path_to_data, "export.xlsx"))

    ###
    # data wrangling
    ###

    ##### CO2

    # clean columns with replacing spaces and replace bricks:
    data_co2.columns = (
        data_co2.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")
    )
    # reduce dimension of emissions in mio. tonnes
    data_co2["CO2_emission"] = data_co2["annual_co₂_emissions_tonnes_"] / 1000000
    # finally drop not needed columns "code" and "annual_co₂_emissions_tonnes_"
    data_co2 = data_co2.drop(["annual_co₂_emissions_tonnes_"], axis=1)
    # rename entity to country_name
    data_co2.rename(columns={"entity": "country_name","code": "country_code","year":"Year"}, inplace=True)

    # %%


    ##### GDP

    # clean columns with replacing spaces and replace bricks:
    data_gdp.columns = (
        data_gdp.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")
    )

    # we see that indicator and also country code are not needed. So lets drop them:
    # finally drop not needed columns "code" and "annual_co₂_emissions_tonnes_"
    data_gdp = data_gdp.drop(["indicator_name", "indicator_code"], axis=1)


    data_gdp = data_gdp.melt(id_vars=["country_name", "country_code"], var_name="Year", value_name="GDP")

    # convert to mil
    data_gdp["GDP"] = np.where(data_gdp["GDP"] != np.nan , data_gdp["GDP"] / 1000000, 0)



    # %%
    # clean population growth

    data_pop_grow.columns = data_pop_grow.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    #we see that indicator and also country code are not needed. So lets drop them:
    #finally drop not needed columns "code" and "annual_co₂_emissions_tonnes_"
    data_pop_grow = data_pop_grow.drop(["indicator_code","indicator_name"],axis=1)


    data_pop_grow = data_pop_grow.melt(id_vars=["country_name", "country_code"], var_name="Year", value_name="population_growth")


    # %%
    # clean population total


    #clean columns with replacing spaces and replace bricks:
    data_pop_total.columns = data_pop_total.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    data_pop_total.rename(columns={"count": "population_total","year":"Year"}, inplace=True)


    # %%
    # clean export

    #clean columns with replacing spaces and replace bricks:
    data_export.columns = data_export.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    data_export.rename(columns={'country':'country_name'}, inplace=True)

    data_export = data_export.melt(id_vars=["country_name"], var_name="Year", value_name="Export")

    # %%

    dfs = [data_export, data_pop_grow, data_gdp, data_pop_total, data_co2]


    #data_pop_total.country_name.nunique()
    # %%

    country_name_code = data_gdp[["country_name","country_code"]].drop_duplicates()
    countries_all = data_export.country_name.drop_duplicates()

    country_table_full = pd.DataFrame(countries_all).merge(country_name_code, on = "country_name", how = "left")


    data_gdp.drop("country_code", axis = 1, inplace = True)

    data_pop_grow.drop("country_code", axis = 1, inplace = True)

    data_co2.drop("country_code", axis = 1, inplace = True)


    # %%


    data_pop_total.Year = data_pop_total.Year.astype(str)
    data_co2.Year = data_co2.Year.astype(str)
    data_pop_grow.Year = data_pop_grow.Year.astype(str)


 

    dfs = [data_export, data_pop_grow, data_gdp, data_pop_total, data_co2]

    df_final = reduce(lambda left,right: pd.merge(left,right,on=["country_name","Year"], how = "left"), dfs)


    # %%

    

 

    # %%

    # creating key:

    country_encoding = {}

    for idx, i in enumerate(country_table_full["country_name"].unique()):
        country_encoding[i] = idx

    country_table_full["country_ID"] = country_encoding.values()
    country_table_full = country_table_full[["country_ID", "country_name", "country_code"]]



    # %%

    df_final["country_name"] = df_final["country_name"].map(country_encoding)

    df_final.rename(columns = {"country_name":"country_ID"}, inplace = True)

    # %%

    
    return df_final, country_table_full


            
def save_data(df_final, country_table_full):
    import sqlite3
    
    path_to_db = os.path.join(Path(__file__).parent,"country_stat.db")
    
    conn = sqlite3.connect(path_to_db)  # You can create a new database by changing the name within the quotes
    c = conn.cursor() # The database will be saved in the location where your 'py' file is saved

    # Create table - Country
    c.execute('''CREATE TABLE Country
                ([country_ID] INTEGER PRIMARY KEY,[country_name] text, [country_code] text)''')
            
    # Create table - Measures
    c.execute('''CREATE TABLE Measures
                ([generated_id] INTEGER PRIMARY KEY,[country_ID] integer, [Year] date, [Export] real, [population_growth] real, [GDP] real, [population_total] real,  [CO2_emission] real)''')

                    
    conn.commit()
    # %%

    # upload to database

    country_table_full.to_sql('Country', conn, if_exists='append', index = False)
    df_final.to_sql('Measures', conn, if_exists='append', index = False)

    return True

      


def main():

    df_final, country_table_full = load_data_and_preprocess()

    output = save_data(df_final, country_table_full)

    if output == True:
        print("\nSuccess!\nDB stored in country_stat.db")


if __name__ == '__main__':
    main()