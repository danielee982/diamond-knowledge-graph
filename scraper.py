from bs4 import BeautifulSoup
import requests
import csv

def scrape_school(school_name, url):
    players = []
    coaches = []
    
    try:
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        
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
                
                bt_elem = bio_items[4] if len(bio_items) > 4 else None
                if bt_elem:
                    bt_wrapper = bt_elem.find('span', attrs={'data-html-wrapper': ''})
                    batting_throwing = bt_wrapper.text.strip() if bt_wrapper else 'N/A'
                else:
                    batting_throwing = 'N/A'
                
                batting = batting_throwing.split('/')[0].strip() if '/' in batting_throwing else 'N/A'
                throwing = batting_throwing.split('/')[1].strip() if '/' in batting_throwing else 'N/A'
                
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
                    'Batting': batting,
                    'Throwing': throwing,
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
    
def write_to_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Define schools to scrape
SCHOOLS = {
    'University of Florida': 'https://floridagators.com/sports/baseball/roster',
    'University of Missouri': 'https://mutigers.com/sports/baseball/roster',
    'University of Oklahoma': 'https://soonersports.com/sports/baseball/roster'
}

all_players_data = []
all_coaches_data = []

if __name__ == '__main__':
    all_players_data = []
    all_coaches_data = []

    for school_name, url in SCHOOLS.items():
        print(f'\nScraping {school_name.upper()}...')
        players, coaches = scrape_school(school_name, url)
        all_players_data.extend(players)
        all_coaches_data.extend(coaches)

    # Write players data to CSV
    write_to_csv('players.csv', all_players_data, ['School', 'Name', 'Jersey', 'Position', 'Class Year', 'Height', 'Weight', 'Batting', 'Throwing', 'High School'])
    print(f'{len(all_players_data)} total player records written to players.csv')

    # Write coaches data to CSV
    write_to_csv('coaches.csv', all_coaches_data, ['School', 'Name', 'Title'])
    print(f'{len(all_coaches_data)} total coach records written to coaches.csv')