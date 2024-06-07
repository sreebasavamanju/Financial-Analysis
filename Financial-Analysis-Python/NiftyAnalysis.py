# Entry Point to Know the NIFTY50 index to find out any better SIP date to invest?
#  Conclusion:  There is no better sip date, consistent and staying for the long term
from Utils import *
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt


def toPandas(path):
    data = []
    with open(path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    df = pd.DataFrame(data)
    df['last_closeRaw'] = df['last_closeRaw'].astype('float64')
    df = df.round(2)
    df['datetime'] = pd.to_datetime(df['rowDate'])
    df = df.sort_values(by='datetime')
    df['day'] = df['datetime'].dt.day
    # print(df.dtypes)
    # print(df)
    fig, ax = plt.subplots()
    for i in range(1, 31):
        local_df = df.loc[df['day'] == i]
        local_df.plot(ax=ax, kind='line', x='datetime', y='last_closeRaw', label=i)

    dates = getListOfDates(startYear=2001, endYear=2024)

    local_df = df.loc[df['datetime'].isin(dates)]
    # print(len(local_df))
    local_df.plot(ax=ax, kind='line', linestyle='dashed', x='datetime', y='last_closeRaw', label='Last Thursday',
                  linewidth=2, color='black')

    plt.legend(loc="upper left", ncol=13)
    plt.xticks(rotation=90)
    plt.title("Nifty50 Index SIP date Wise Investment")
    plt.xlabel("Date")
    plt.ylabel("Nifty50 Index")
    plt.grid()
    plt.show()


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Invalid Number of arguments")
        print("Usage: python NiftyAnalysis.py <NIFTY50 Json Path>")
        sys.exit(1)
    path = sys.argv[1]
    toPandas(path)

