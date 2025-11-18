from bs4 import BeautifulSoup
import requests
import csv
import time
from funcs_neo4j import GraphDBManager

def scrape_school(school_name, url):
    players, coaches = [], []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        html = requests.get(url, headers=headers, timeout=15).text
        soup = BeautifulSoup(html, 'html.parser')

        # Detect structure automatically
        if soup.find('div', class_='s-person-card__content'):
            print(f"Detected Sidearm layout for {school_name}")
            players, coaches = parse_sidearm(school_name, soup)
        elif soup.find('div', class_='table--roster'):
            print(f"Detected Table layout for {school_name}")
            players, coaches = parse_table_roster(school_name, soup)
        elif soup.find('li', class_='sidearm-roster-player'):
            print(f"Detected Classic Sidearm layout for {school_name}")
            players, coaches = parse_sidearm_classic(school_name, soup)
        else:
            print(f"Unknown structure for {school_name}, skipping.")

    except Exception as e:
        print(f"*** Error scraping {school_name}: {e}")

    return players, coaches

def parse_sidearm(school_name, soup):
    players = []
    coaches = []
    
    try:
        all_people = soup.find_all('div', attrs={'class': 's-person-card__content'})
        
        for person in all_people:
            name = person.find('h3')
            if name is None:
                continue
            
            # Player
            if person.find('div', attrs={'class': 's-person-details__bio-stats'}):
                
                # Get jersey number
                jersey_elem = person.find('div', attrs={'data-test-id': 's-stamp__root'})
                if jersey_elem:
                    number_span = jersey_elem.find('span', class_='s-stamp__text')
                    if number_span:
                        sr_only = number_span.find('span', class_='sr-only')
                        if sr_only:
                            sr_only.decompose()
                        jersey = number_span.text.strip()
                    else:
                        jersey = 'N/A'
                else:
                    jersey = 'N/A'
                
                bio_items = person.find_all('span', attrs={'class': 's-person-details__bio-stats-item'})
                
                position_elem = bio_items[0] if len(bio_items) > 0 else None
                if position_elem:
                    sr_only = position_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    position = position_elem.text.strip()
                else:
                    position = 'N/A'
                
                class_year_elem = bio_items[1] if len(bio_items) > 1 else None
                if class_year_elem:
                    sr_only = class_year_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    class_year = class_year_elem.text.strip()
                else:
                    class_year = 'N/A'
                
                height_elem = bio_items[2] if len(bio_items) > 2 else None
                if height_elem:
                    sr_only = height_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    height = height_elem.text.strip()
                else:
                    height = 'N/A'
                
                weight_elem = bio_items[3] if len(bio_items) > 3 else None
                if weight_elem:
                    sr_only = weight_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    weight = weight_elem.text.strip()
                else:
                    weight = 'N/A'
                
                # bt_elem = bio_items[4] if len(bio_items) > 4 else None
                # if bt_elem:
                #     bt_wrapper = bt_elem.find('span', attrs={'data-html-wrapper': ''})
                #     batting_throwing = bt_wrapper.text.strip() if bt_wrapper else 'N/A'
                # else:
                #     batting_throwing = 'N/A'
                
                # batting = batting_throwing.split('/')[0].strip() if '/' in batting_throwing else 'N/A'
                # throwing = batting_throwing.split('/')[1].strip() if '/' in batting_throwing else 'N/A'
                
                highschool_elem = person.find('span', attrs={'data-test-id': 's-person-card-list__content-location-person-high-school'})
                if highschool_elem:
                    sr_only = highschool_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    highschool = highschool_elem.text.strip()
                else:
                    highschool = 'N/A'
                
                players.append({
                    'School': school_name.title(),
                    'Name': name.text.strip(),
                    'Jersey': jersey,
                    'Position': position,
                    'Class Year': class_year,
                    'Height': height,
                    'Weight': weight,
                    # 'Batting': batting,
                    # 'Throwing': throwing,
                    'High School': highschool
                })
            
            # Coach
            else:
                title_elem = person.find('div', attrs={'class': 's-person-details__position'})
                if title_elem:
                    title_div = title_elem.find('div')
                    title = title_div.text.strip() if title_div else 'N/A'
                else:
                    title = 'N/A'
                
                coaches.append({
                    'School': school_name.title(),
                    'Name': name.text.strip(),
                    'Title': title
                })
        
        print(f'{school_name.upper()} scraped successfully')
        return players, coaches
        
    except Exception as e:
        print(f'*** Error scraping {school_name.upper()}: {str(e)}')
        return [], []
    
def parse_sidearm_classic(school_name, soup):
    """
    Parse the older/classic Sidearm Sports roster layout.
    Extracts players and coaches in the SAME FORMAT as existing CSV output.
    """
    players = []
    coaches = []

    # parse players
    player_items = soup.find_all("li", class_="sidearm-roster-player")
    for item in player_items:
        try:
            name_tag = item.find("h3")
            name = name_tag.get_text(strip=True) if name_tag else "N/A"

            # Jersey #
            jersey_tag = item.find("span", class_="sidearm-roster-player-jersey-number")
            jersey = jersey_tag.get_text(strip=True) if jersey_tag else "N/A"

            # Position
            pos_tag = item.find("span", class_="sidearm-roster-player-position-long-short")
            position = pos_tag.get_text(strip=True) if pos_tag else "N/A"

            # Height / Weight
            height_tag = item.find("span", class_="sidearm-roster-player-height")
            height = height_tag.get_text(strip=True) if height_tag else "N/A"

            weight_tag = item.find("span", class_="sidearm-roster-player-weight")
            weight = weight_tag.get_text(strip=True) if weight_tag else "N/A"

            # High School
            hs_tag = item.find("span", class_="sidearm-roster-player-highschool")
            highschool = hs_tag.get_text(strip=True) if hs_tag else "N/A"

            # Class year
            class_tag = item.find("span", class_="sidearm-roster-player-academic-year")
            class_year = class_tag.get_text(strip=True) if class_tag else "N/A"

            players.append({
                "School": school_name,
                "Name": name,
                "Jersey": jersey,
                "Position": position,
                "Class Year": class_year,
                "Height": height,
                "Weight": weight,
                "High School": highschool,
            })

        except Exception as e:
            print(f"Error parsing player in {school_name}: {e}")
            continue

    # parse coaches
    coach_items = soup.find_all("li", class_="sidearm-roster-coach")
    for item in coach_items:
        try:
            name_tag = item.find("p")
            name = name_tag.get_text(strip=True) if name_tag else "N/A"

            title_tag = item.find("div", class_="sidearm-roster-coach-title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            coaches.append({
                "School": school_name,
                "Name": name,
                "Title": title,
            })

        except Exception as e:
            print(f"Error parsing coach in {school_name}: {e}")
            continue

    print(f"{school_name.upper()} (classic layout) scraped successfully")
    return players, coaches
    
def parse_table_roster(school_name, soup):
    players, coaches = [], []

    # Find all roster tables (covers both players and coaches)
    tables = soup.find_all("div", class_="table--roster")
    for table in tables:
        tbody = table.find("tbody")
        if not tbody:
            continue

        for row in tbody.find_all("tr"):
            # Extract all cells (both td and th)
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            
            # Extract plain text from each cell
            cols = []
            for cell in cells:
                a = cell.find("a")
                text = a.get_text(strip=True) if a else cell.get_text(strip=True)
                cols.append(text)
            
            # --- COACH rows ---
            if len(cols) <= 2:
                name = cols[0] if len(cols) >= 1 else "N/A"
                title = cols[1] if len(cols) >= 2 else "N/A"
                coaches.append({
                    "School": school_name,
                    "Name": name,
                    "Title": title
                })
                continue

            # --- PLAYER row ---
            # [#, Name, Class, Pos, Height, Weight, B/T, Hometown, High School, ...]
            jersey = cols[0] if len(cols) > 0 else "N/A"
            name = cols[1] if len(cols) > 1 else "N/A"
            class_year = cols[2] if len(cols) > 2 else "N/A"
            position = cols[3] if len(cols) > 3 else "N/A"
            height = cols[4] if len(cols) > 4 else "N/A"
            weight = cols[5] if len(cols) > 5 else "N/A"
            # bt = cols[6] if len(cols) > 6 else "N/A"
            highschool = cols[8] if len(cols) > 8 else "N/A"

            # Split Bat/Throw if formatted as "R/R"
            # batting, throwing = "N/A", "N/A"
            # if "/" in bt:
            #     parts = bt.split("/")
            #     batting = parts[0].strip()
            #     throwing = parts[1].strip() if len(parts) > 1 else "N/A"

            players.append({
                "School": school_name,
                "Name": name,
                "Jersey": jersey,
                "Class Year": class_year,
                "Position": position,
                "Height": height,
                "Weight": weight,
                # "Batting": batting,
                # "Throwing": throwing,
                "High School": highschool,
            })
    print(f'{school_name.upper()} scraped successfully')
    return players, coaches

def write_to_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Schools to scrape
SCHOOLS = {
    'University of Florida': 'https://floridagators.com/sports/baseball/roster',
    'University of Missouri': 'https://mutigers.com/sports/baseball/roster',
    'University of Oklahoma': 'https://soonersports.com/sports/baseball/roster',
    'University of Alabama': 'https://rolltide.com/sports/baseball/roster/2025',
    'University of Washington': 'https://gohuskies.com/sports/baseball/roster/2025',
    'University of Oregon': 'https://goducks.com/sports/baseball/roster/2025',
    'University of Indiana': 'https://iuhoosiers.com/sports/baseball/roster/2025',
    'University of Minnesota': 'https://gophersports.com/sports/baseball/roster/2025',
    'Texas A&M University': 'https://12thman.com/sports/baseball/roster/2025',
    'University of Mississippi': 'https://olemisssports.com/sports/baseball/roster/2025',

    'University of Maryland': 'https://umterps.com/sports/baseball/roster/2025',
    'Rutgers University': 'https://scarletknights.com/sports/baseball/roster/2025',
}

all_players_data = []
all_coaches_data = []

if __name__ == '__main__':
    all_players_data = []
    all_coaches_data = []
    high_schools = set()

    for i, (school_name, url) in enumerate(SCHOOLS.items()):
        print(f'\nScraping {school_name.upper()}...')

        if i > 0:
            print(f"*** Waiting 3 second before next request to avoid rate limiting...")
            time.sleep(3)

        players, coaches = scrape_school(school_name, url)
        all_players_data.extend(players)
        all_coaches_data.extend(coaches)

        for player in players:
            if player["High School"] != 'N/A':
                high_schools.add(player["High School"])

    # Write players data to CSV
    write_to_csv('players.csv', all_players_data, ['School', 'Name', 'Jersey', 'Position', 'Class Year', 'Height', 'Weight', 'High School'])
    print(f'{len(all_players_data)} total player records written to players.csv')

    # Write coaches data to CSV
    write_to_csv('coaches.csv', all_coaches_data, ['School', 'Name', 'Title'])
    print(f'{len(all_coaches_data)} total coach records written to coaches.csv')

    # Write schools data to CSV
    with open('schools.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'school type'])  # Header
        
        # Write universities
        for school_name in SCHOOLS.keys():
            writer.writerow([school_name, 'university'])
        
        # Write high schools
        for hs in sorted(high_schools):
            writer.writerow([hs, 'high school'])
    print(f'{len(high_schools)} total high schools written to schools.csv')