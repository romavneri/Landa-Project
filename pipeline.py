import sys
import requests
import sqlite3
import schedule
import pandas as pd
from geopy.geocoders import Nominatim

url = "https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4"

# region Create DB and table
conn = sqlite3.connect("test.db")
conn.execute(
    """ 
    create table my_table as 
    select * from my_table
    """)
# endregion


def get_data() -> pd.DataFrame:
    """
    Get data from the url
    ** Not working
    :return: Dataframe of the data scraped from the website
    """
    r = requests.get(url)
    df_list = pd.read_html(r.text)
    df = df_list[0]
    return df


def to_geo_code(data: pd.DataFrame) -> pd.DataFrame:
    """
    Gets Dataframe and tries to convert its address to lat/long
    :param data: Original Dataframe
    :return: Dataframe with additional lat and long columns
    """
    geolocator = Nominatim(user_agent="my_user_agent")
    l1 = []
    l2 = []
    for idx, row in data.iterrows():
        address = row["House No"] + " " + row["Street Name"] + ", " + row["Borough"]
        loc = geolocator.geocode(address)
        try:
            l1.append(loc.latitude)
            l2.append(loc.longitude)
        except AttributeError:
            l1.append("")
            l2.append("")
    data["latitude"] = l1
    data["longtitude"] = l2
    return data


def pipeline():
    """
    Pipeline that executes all the functions
    :return: None
    """
    global conn
    data = get_data()
    data = to_geo_code(data)
    data.to_sql("my_table", conn, if_exists="replace")


def runner(time: int):
    """
    Run the pipeline every interval
    :param time: Interval time
    :return: None
    """
    schedule.every(time).minutes.do(pipeline)
    while True:
        try:
            schedule.run_pending()
        except KeyboardInterrupt:
            print("Good Bye")
            sys.exit(0)


if __name__ == "__main__":
    try:
        runner(int(input("Insert interval time (should be integer)")))
    except ValueError:
        print("Enter a number")
        runner(int(input("Insert interval time")))

    print("User didn't enter a number. Exiting...")
    sys.exit(-1)
