# This could be migrated to go. I wrote this when I was implementing the service with Java.

import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime

YEAR = 2019

resp = requests.get('http://rotoguru1.com/cgi-bin/mlbsched4.cgi?v=0&start=&weeks=50')

if resp.status_code == 200:
    soup = BeautifulSoup(resp.content, features='html.parser')
    rows = soup.find_all('tr')

    # Team list corresponds to the columns in the table
    team_list = []
    for row in rows:
        headers = row.find_all('th')
        if headers:
            for header in headers:
                team_list.append(header.getText())
            break
    teams = list(filter(lambda t: t != 'Date', team_list))

    # {team_name: [(away_team, date)]}
    home_team_games = dict(map(lambda t: (t, []), teams))

    # Iterate through table, add home games and date to the dictionary
    for col in rows:
        cells = col.find_all('td')
        if cells:
            cell_list = list(map(lambda c: c.getText(), cells))
            date = cell_list[0]
            games = list(filter(lambda g: g != date, cell_list))
            mm_dd_arr = date.split('-')[1].split('/')
            month = int(mm_dd_arr[0])
            day = int(mm_dd_arr[1])
            timestamp_seconds_millis = int(datetime(YEAR, month, day).timestamp()) * 1000

            for i, v in enumerate(games):
                # print(games)
                if v != '\xa0' and not '@' in v:
                    home_team_games[teams[i]].append((v, timestamp_seconds_millis))

home_team_mappings = {
    'Ana': ('Los Angeles Angels of Anaheim', 33.799925, -117.883194),
    'Ari': ('Arizona Diamondbacks', 33.445526, -112.066721),
    'Atl': ('Atlanta Braves', 33.734805, -84.389996),
    'Bal': ('Baltimore Orioles', 39.283964, -76.621618),
    'Bos': ('Boston Red Sox', 42.346619, -71.096961),
    'ChC': ('Chicago Cubs', 41.947856, -87.655887),
    'ChW': ('Chicago White Sox', 41.829908, -87.633540),
    'Cin': ('Cincinnati Reds', 39.097935, -84.508158),
    'Cle': ('Cleveland Indians', 41.496192, -81.685238),
    'Col': ('Colorado Rockies', 39.755891, -104.994198),
    'Det': ('Detroit Tigers', 42.339227, -83.049506),
    'Fla': ('Miami Marlins', 25.778655, -80.220305),
    'Hou': ('Houston Astros', 29.756965, -95.354824),
    'Kan': ('Kansas City Royals', 39.051098, -94.481115),
    'Los': ('Los Angeles Dodgers', 34.072724, -118.240646),
    'Mil': ('Milwaukee Brewers', 43.027982, -87.971165),
    'Min': ('Minnesota Twins', 44.981713, -93.277347),
    'NYM': ('New York Mets', 40.756337, -73.846043),
    'NYY': ('New York Yankees', 40.829327, -73.927735),
    'Oak': ('Oakland Athletics', 37.751605, -122.200523),
    'Phi': ('Philadelphia Phillies', 39.905547, -75.166589),
    'Pit': ('Pittsburgh Pirates', 40.447307, -80.006841),
    'Sdg': ('San Diego Padres', 32.707710, -117.157097),
    'Sea': ('Seattle Mariners', 47.591358, -122.332283),
    'Sfo': ('San Francisco Giants', 37.778473, -122.389595),
    'StL': ('St. Louis Cardinals', 38.622317, -90.193891),
    'Tam': ('Tampa Bay Rays', 27.768160, -82.653465),
    'Tex': ('Texas Rangers', 32.751147, -97.082454),
    'Tor': ('Toronto Blue Jays', 43.641111, -79.389675),
    'Was': ('Washington Nationals', 38.873010, -77.007457)
}
data = []
for k, v in home_team_games.items():
    for tup in v:
        team_data = home_team_mappings[k]
        data.append(
            {
                'home_team': k.upper(),
                'away_team': tup[0].upper(),
                'game_date_millis': tup[1],
                'full_team_name': team_data[0],
                'stadium_location': {'lat': team_data[1], 'lon': team_data[2]}
            })

# Write games out in jsonl format. Each line represents a unique game.
with open('data/mlb-games.jsonl', 'w') as outfile:
    for line in data:
        json.dump(line, outfile)
        outfile.write('\n')
