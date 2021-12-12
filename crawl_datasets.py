'''
Script that crawls all datasets described in: https://docs.google.com/document/d/1hg4mRc8rdxw39dtCCNdIluewv2-U13daaaEuOafplas/edit#
'''

## Imports
import requests, os, zipfile, webbrowser, time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from tqdm import tqdm

def get_all_links(link:str, html:BeautifulSoup) -> (str):
    '''
    go through all possible years of given desired dataset - works only for "https://www.numbeo.com"
    Parameter:
        - link: raw link of the numbeo dataset [String]
        - html: html object of this link [bs4.BeautifulSoup]
    Returns:
        - links: list with all links to all available years of given dataset [List(String)]
    '''
    ## init list of years
    years = []
    ## go through all available years
    for raw_year in html.find(class_ = "changePageForm").findAll("option"):
        ## don't take half year results
        if "Mid-Year" not in raw_year.text:
            ## add respective year to list of years
            years.append(raw_year.attrs["value"])
    ## construct the complete link for every year
    links = [f'{link}?title={year}' for year in years]
    return links

def read_table(table:BeautifulSoup) -> pd.DataFrame:
    '''
    reads content of given table html object
    Parameter:
        - table: the table to parse from [bs4.BeautifulSoup]
    Returns:
        - data: data of the given table [pandas.DataFrame]
    '''
    ## init the list of columns
    columns = []
    ## columns are in the thead
    for con in table.thead.tr.children:
        if con.name != None:
            columns.append(con.text)
    ## init list of dataframes - one per line (=country)
    dataframes = []
    ## init rank
    i = 0
    ## countries are in tbody
    for country in table.tbody.children:
        if country.name != None:
            ## init the data per country
            data_as_list = []
            for child in country.children:
                if child.name != None:
                    data_as_list.append(child.text)
            i+=1
            ## set the rank of this country
            data_as_list[0] = i
            ## convert to DataFrame
            dataframes.append(pd.DataFrame(np.array(data_as_list).reshape(1,-1), columns = columns))
    ## concat all DataFrames
    data = pd.concat(dataframes).reset_index(drop = True)
    ## convert all columns to numeric data, if possible
    for col in columns:
        data[col] = pd.to_numeric(data[col], errors='ignore')
    return data
    
def get_data(link:str) -> pd.DataFrame:
    '''
    crawls desired info from given link - only works for https://www.numbeo.com
    Parameter:
        - link: link to desired website [String]
    Returns:
        - data: parsed data [pandas.DataFrame]
    '''
    ## get request
    request = requests.get(link)
    ## get html content
    html = BeautifulSoup(request.content, "html.parser")
    ## search for tables
    tables = html.findAll("table")
    ## for numbeo we always want the second table
    table = tables[1]
    ## read it
    data = read_table(table)
    return data    

def extract_zip(filename:str) -> None:
    '''
    extracts desired file from given zip file - only works for downloaded zip files from "https://api.worldbank.org"
    Parameter:
        - filename: given zip file [String]
    Returns:
        - None
    '''
    ## open zip file
    with zipfile.ZipFile(filename, 'r') as archive:
        ## we need the csv file starting with "API_"
        file = [f for f in archive.namelist() if (f.startswith("API_")) and (f.endswith(".csv"))][0]
        ## extract desired file
        archive.extract(file, '')
    ## rename it (that we know what exactly is in it)
    os.rename(file, f'{filename[:-4]}.csv')
    ## get rid of zip file. We don't need it anymore
    os.remove(filename)
    
    
if __name__ == "__main__":
    ## check if we in correct folder
    if os.getcwd() != "data/":
        ## check whether "data/" folder already there
        if not os.path.isdir("data/"):
            ## add "data/" directory
            os.mkdir("data/")
    ## change to go to above folder, because we are in "data/" then
    else:
        os.chdir("../")
    ## read the list of links
    with open("dataset_links.txt", "r") as f:
        links = f.read().split("\n")
    ## if github links already exists, remove, make it new
    if "github_links.txt" in os.listdir():
        os.remove("github_links.txt")
    gitlinks = open("github_links.txt", "w")
    ## change to "data/" directory
    os.chdir("data")
    ## go through links
    for raw in links:
        ## split by " " --> "dataset_links.txt" is build like '<link> <name>' 
        link, name = raw.split(" ")
        if "worldbank" in link:
            if f'{name}.csv' not in os.listdir():
                ## download zip file
                r = requests.get(link, allow_redirects = True)
                open(f"{name}.zip", "wb").write(r.content)
                ## extract desired csv from zip file
                extract_zip(f'{name}.zip')
        elif "oecd" in link:
            if f'{name}.csv' not in os.listdir():
                ## download csv file
                r = requests.get(link, allow_redirects = True)
                open(f"{name}.csv", "wb").write(r.content)
        elif "numbeo" in link:
            if f'{name}.csv' not in os.listdir():
                ## parse html content from site
                request = requests.get(link)
                html = BeautifulSoup(request.content, "html.parser")
                ## get all available years
                year_links = get_all_links(link, html)
                ## init list of data
                data = []
                ## go through the links of years
                for l in tqdm(year_links):
                    ## get data
                    raw_data = get_data(link)
                    ## add year information
                    raw_data["Year"] = int(l.split("=")[-1].split("-")[0])
                    data.append(raw_data)
                ## concat all DataFrames
                whole_data = pd.concat(data)
                ## write to csv
                whole_data.to_csv(f'{name}.csv')
        else:
            if f'{name}.csv' not in os.listdir():
                ## these cannot be downloaded automatically unfortunately
                print(f"data not automatically downloadable. Please go to {link} and download the dataset manually and rename the downloaded file into {name}.csv")
                ## So we wait 3s and open the link then
                time.sleep(3)
                webbrowser.open(link)
        ## add link, write to file
        gitlink = f"https://raw.githubusercontent.com/papstchaka/dopp_datasets/master/data/{name}.csv\n"
        gitlinks.write(gitlink)
    gitlinks.close()