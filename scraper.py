from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd

def scrape_school(school_name, url, season):
    players, coaches = [], []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.6367.207 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        html = requests.get(url, headers=headers, timeout=15).text
        soup = BeautifulSoup(html, 'html.parser')

        # Detect structure automatically
        if soup.find('div', class_='s-person-card__content'):
            print(f"Detected Sidearm layout for {school_name}")
            players, coaches = parse_sidearm(school_name, soup, season)
        elif soup.find('li', class_='sidearm-roster-player'):
            print(f"Detected Classic Sidearm layout for {school_name}")
            players, coaches = parse_sidearm_classic(school_name, soup, season)
        else:
            print(f"Unknown structure for {school_name}, skipping.")

    except Exception as e:
        print(f"*** Error scraping {school_name}: {e}")

    return players, coaches

def parse_sidearm(school_name, soup, season):
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
                    feet, inch = height.split("'")[0], height.split("'")[1].replace('"', '')
                    height = 12 * int(feet) + int(inch)
                else:
                    height = 'N/A'
                
                weight_elem = bio_items[3] if len(bio_items) > 3 else None
                if weight_elem:
                    sr_only = weight_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    weight = weight_elem.text.strip()
                    weight = weight.split(' ')[0]  # Leave only the number
                else:
                    weight = 'N/A'
                
                bt_elem = bio_items[4] if len(bio_items) > 4 else None
                if bt_elem:
                    bt_wrapper = bt_elem.find('span', attrs={'data-html-wrapper': ''})
                    batting_throwing = bt_wrapper.text.strip() if bt_wrapper else 'N/A'
                else:
                    batting_throwing = 'N/A'
                
                high_school_elem = person.find('span', attrs={'data-test-id': 's-person-card-list__content-location-person-high-school'})
                if high_school_elem:
                    sr_only = high_school_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    high_school = high_school_elem.text.strip()
                else:
                    high_school = 'N/A'
                
                hometown_elem = person.find('span', attrs={'data-test-id': 's-person-card-list__content-location-person-hometown'})
                if hometown_elem:
                    sr_only = hometown_elem.find('span', class_='sr-only')
                    if sr_only:
                        sr_only.decompose()
                    hometown = hometown_elem.text.strip()
                else:
                    hometown = 'N/A'
                
                players.append({
                    'College': school_name,
                    'Name': name.text.strip(),
                    'Jersey': jersey,
                    'Position': position,
                    'Class Year': class_year,
                    'Height': height,
                    'Weight': weight,
                    'B/T': batting_throwing,
                    'High School': high_school,
                    'Hometown': hometown,
                    "Season": season
                })
            
            # Coach
            else:
                title_elem = person.find('div', attrs={'class': 's-person-details__position'})
                if title_elem:
                    title_div = title_elem.find('div')
                    title = title_div.text.strip() if title_div else 'N/A'
                else:
                    title = 'N/A'

                if 'coach' not in title.lower():
                    continue
                
                coaches.append({
                    'College': school_name,
                    'Name': name.text.strip(),
                    'Title': title,
                    "Season": season
                })
        
        print(f'{school_name.upper()} scraped successfully')
        return players, coaches
        
    except Exception as e:
        print(f'*** Error scraping {school_name.upper()}: {str(e)}')
        return [], []
    
def parse_sidearm_classic(school_name, soup, season):
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
            feet, inch = height.split("'")[0], height.split("'")[1].replace('"', '')
            height = 12 * int(feet) + int(inch)

            weight_tag = item.find("span", class_="sidearm-roster-player-weight")
            weight = weight_tag.get_text(strip=True) if weight_tag else "N/A"
            weight = weight.split(' ')[0]  # Leave only the number

            # Batting/Throwing
            bt_tag = item.find("span", class_="sidearm-roster-player-bats-throws")
            bt_text = bt_tag.get_text(strip=True) if bt_tag else "N/A"

            # High School
            high_school_tag = item.find("span", class_="sidearm-roster-player-highschool")
            high_school = high_school_tag.get_text(strip=True) if high_school_tag else "N/A"

            # Class year
            class_tag = item.find("span", class_="sidearm-roster-player-academic-year")
            class_year = class_tag.get_text(strip=True) if class_tag else "N/A"

            # Hometown
            hometown_tag = item.find("span", class_="sidearm-roster-player-hometown")
            hometown = hometown_tag.get_text(strip=True) if hometown_tag else "N/A"

            players.append({
                "College": school_name,
                "Name": name,
                "Jersey": jersey,
                "Position": position,
                "Class Year": class_year,
                "Height": height,
                "Weight": weight,
                "B/T": bt_text,
                "High School": high_school,
                "Hometown": hometown,
                "Season": season
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

            if 'coach' not in title.lower():
                continue

            coaches.append({
                "College": school_name,
                "Name": name,
                "Title": title,
                "Season": season
            })

        except Exception as e:
            print(f"Error parsing coach in {school_name}: {e}")
            continue

    print(f"{school_name.upper()} (classic layout) scraped successfully")
    return players, coaches

def write_to_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Schools to scrape
SCHOOLS = {
    'University of Florida': 'https://floridagators.com/sports/baseball/roster',
    'University of Missouri': 'https://mutigers.com/sports/baseball/roster/2025',
    'University of Oklahoma': 'https://soonersports.com/sports/baseball/roster/2025',
    'University of Alabama': 'https://rolltide.com/sports/baseball/roster/2025',
    'University of Washington': 'https://gohuskies.com/sports/baseball/roster/2025',
    'University of Oregon': 'https://goducks.com/sports/baseball/roster/2025',
    'University of Indiana': 'https://iuhoosiers.com/sports/baseball/roster/2025',
    'University of Minnesota': 'https://gophersports.com/sports/baseball/roster/2025',
    'Texas A&M University': 'https://12thman.com/sports/baseball/roster/2025',
    'University of Southern Mississippi': 'https://southernmiss.com/sports/baseball/roster/2025',
    'Troy University': 'https://troytrojans.com/sports/baseball/roster/2025?path=baseball',
    'University of Louisiana at Lafayette': 'https://ragincajuns.com/sports/baseball/roster/2025',
    'Rice University': 'https://riceowls.com/sports/baseball/roster/2025',
    'University of Memphis': 'https://gotigersgo.com/sports/baseball/roster/2025',
    'University of North Carolina at Charlotte': 'https://charlotte49ers.com/sports/baseball/roster/2025',
    'Oregon State University': 'https://osubeavers.com/sports/baseball/roster/2025',
    'Texas Tech University': 'https://texastech.com/sports/baseball/roster/2025',
    'Oklahoma State University': 'https://okstate.com/sports/baseball/roster/2025',
    'Fresno State University': 'https://gobulldogs.com/sports/baseball/roster/2025',
    'Air Force Academy': 'https://goairforcefalcons.com/sports/baseball/roster/2025',
}

SCHOOLS_2024 = {
    'University of Florida': 'https://floridagators.com/sports/baseball/roster/2024',
    'University of Missouri': 'https://mutigers.com/sports/baseball/roster/2024',
    'University of Oklahoma': 'https://soonersports.com/sports/baseball/roster/2024',
    'University of Alabama': 'https://rolltide.com/sports/baseball/roster/2024',
    'University of Washington': 'https://gohuskies.com/sports/baseball/roster/2024',
    'University of Oregon': 'https://goducks.com/sports/baseball/roster/2024',
    'University of Indiana': 'https://iuhoosiers.com/sports/baseball/roster/2024',
    'University of Minnesota': 'https://gophersports.com/sports/baseball/roster/2024',
    'Texas A&M University': 'https://12thman.com/sports/baseball/roster/2024',
    'University of Southern Mississippi': 'https://southernmiss.com/sports/baseball/roster/2024',
    'Troy University': 'https://troytrojans.com/sports/baseball/roster/2024?path=baseball',
    'University of Louisiana at Lafayette': 'https://ragincajuns.com/sports/baseball/roster/2024',
    'Rice University': 'https://riceowls.com/sports/baseball/roster/2024',
    'University of Memphis': 'https://gotigersgo.com/sports/baseball/roster/2024',
    'University of North Carolina at Charlotte': 'https://charlotte49ers.com/sports/baseball/roster/2024',
    'Oregon State University': 'https://osubeavers.com/sports/baseball/roster/2024',
    'Texas Tech University': 'https://texastech.com/sports/baseball/roster/2024',
    'Oklahoma State University': 'https://okstate.com/sports/baseball/roster/2024',
    'Fresno State University': 'https://gobulldogs.com/sports/baseball/roster/2024',
    'Air Force Academy': 'https://goairforcefalcons.com/sports/baseball/roster/2024',
}

if __name__ == '__main__':
    all_players_data = []
    all_coaches_data = []
    high_schools = set()

    for i, (school_name, url) in enumerate(SCHOOLS.items()):
        print(f'\nScraping {school_name.upper()} 2025...')

        players, coaches = scrape_school(school_name, url, season=2025)
        all_players_data.extend(players)
        all_coaches_data.extend(coaches)

        for player in players:
            if player["High School"] != 'N/A':
                high_schools.add(player["High School"].replace("/", "").strip())

    for i, (school_name, url) in enumerate(SCHOOLS_2024.items()):
        print(f'\nScraping {school_name.upper()} 2024...')

        players, coaches = scrape_school(school_name, url, season=2024)
        all_players_data.extend(players)
        all_coaches_data.extend(coaches)

        for player in players:
            if player["High School"] != 'N/A':
                high_schools.add(player["High School"].replace("/", "").strip())

    # Write players data to CSV
    write_to_csv('data/raw/players.csv', all_players_data, ['College', 'Name', 'Jersey', 'Position', 'Class Year', 'Height', 'Weight', 'B/T', 'High School', 'Hometown', 'Season'])
    print(f'{len(all_players_data)} total player records written to players.csv')

    # Write coaches data to CSV
    write_to_csv('data/raw/coaches.csv', all_coaches_data, ['College', 'Name', 'Title', 'Season'])
    print(f'{len(all_coaches_data)} total coach records written to coaches.csv')

    # Write schools data to CSV
    with open('data/raw/highschools.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['name'])  # Header
        
        # Write high schools
        for hs in sorted(high_schools):
            writer.writerow([hs])
    print(f'{len(high_schools)} total high schools written to highschools.csv')

    with open('data/processed/colleges.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['name'])

        for univ, _ in SCHOOLS.items():
            writer.writerow([univ])