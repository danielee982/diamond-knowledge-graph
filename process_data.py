from rapidfuzz import fuzz, process
import re
import pandas as pd

def dedup_last_schools(players_df, lastschools_df):
    canonical_hs = lastschools_df['name'].unique()

    ls_names = players_df['Last School'].unique()

    ls_mapping = {}

    for school in ls_names:
        matches = process.extract(
            school,
            canonical_hs,
            scorer=fuzz.partial_ratio,
            score_cutoff=70,
        )
        if matches:
            ls_mapping[school] = matches[0][0]
        else:
            ls_mapping[school] = school

    players_df['Last School'] = players_df['Last School'].map(ls_mapping).fillna(players_df['Last School'])
    lastschools_df['name'] = lastschools_df['name'].map(ls_mapping).fillna(lastschools_df['name'])
    
    lastschools_df.drop_duplicates(subset=['name'], inplace=True)

    print(f"Original: {len(ls_names)}")
    print(f"Canonical: {len(set(ls_mapping.values()))}")
    print(f"After deduplication: {len(ls_names) - len(set(ls_mapping.values()))} duplicates removed.")

    return players_df, lastschools_df

def dedup_coaches(coaches_df):
    # 1. John Smith at ABC Univ: Head Coach
    # 2. John Smith at ABC Univ: Recruiting Coordinator
    # => 1. John Smith at ABC Univ: Head Coach | Recruiting Coordinator
    coaches_clean = coaches_df.groupby(['Name', 'College'], as_index=False).agg({
        'Title': lambda x: ' | '.join(sorted(set(x)))
    })
    return coaches_clean

def extract_positions(pos_str, mapping):
    parts = re.split(r'[/,|]', pos_str)
    return [mapping.get(p) for p in parts if p]

def standardize_player_positions(players_df):
    mapping = {
        "OF": "Outfielder",
        "INF": "Infielder",
        "C": "Catcher",
        "RHP": "Right-Handed Pitcher",
        "LHP": "Left-Handed Pitcher",
        "UTIL": "Utility",
        "UTL": "Utility",
        "1B": "First Base",
        "2B": "Second Base",
        "3B": "Third Base",
        "SS": "Shortstop",
        "DH": "Designated Hitter",
    }

    players_df['Position List'] = players_df['Position'].apply(lambda x: extract_positions(x, mapping))
    max_len = players_df['Position List'].apply(len).max()

    for i in range(max_len):
        players_df[f'position{i+1}'] = players_df['Position List'].apply(
            lambda x: x[i] if i < len(x) else None
        )

    return players_df

def standardize_class_year(players_df):
    mapping = {
        'Jr.': 'Junior',
        'Sr.': 'Senior',
        'So.': 'Sophomore',
        'R-Fr.': 'Redshirt Freshman',
        'Fr.': 'Freshman',
        'R-Jr.': 'Redshirt Junior',
        'R-So.': 'Redshirt Sophomore',
        'Gr.': 'Graduate',
        'R-Sr.': 'Redshirt Senior',
        'Gr.+': 'Graduate',
    }

    players_df['Class Year'] = players_df['Class Year'].apply(lambda x: mapping.get(x))
    
    return players_df

def map_team(players_df, coaches_df):
    teams_mapping = {
        'University of Florida': 'Florida Gators',
        'University of Missouri': 'Missouri Tigers',
        'University of Oklahoma': 'Oklahoma Sooners',
        'University of Alabama': 'Alabama Crimson Tide',
        'University of Washington': 'Washington Huskies',
        'University of Oregon': 'Oregon Ducks',
        'University of Indiana': 'Indiana Hoosiers',
        'University of Minnesota': 'Minnesota Golden Gophers',
        'Texas A&M University': 'Texas A&M Aggies',
        'University of Southern Mississippi': 'Southern Miss Golden Eagles',
        'Troy University': 'Troy Trojans',
        'University of Louisiana at Lafayette': 'Louisiana Ragin\' Cajuns',
        'Rice University': 'Rice Owls',
        'University of Memphis': 'Memphis Tigers',
        'University of North Carolina at Charlotte': 'Charlotte 49ers',
        'Oregon State University': 'Oregon State Beavers',
        'Texas Tech University': 'Texas Tech Red Raiders',
        'Oklahoma State University': 'Oklahoma State Cowboys',
        'Fresno State University': 'Fresno State Bulldogs',
        'Air Force Academy': 'Air Force Falcons',
    }

    players_df['Team'] = players_df['College'].apply(lambda x: teams_mapping.get(x))
    coaches_df['Team'] = coaches_df['College'].apply(lambda x: teams_mapping.get(x))

    return players_df, coaches_df

def standardize_batting_throwing(players_df):
    players_df['B/T'] = players_df['B/T'].fillna('N/A').astype(str)
    players_df['B/T'] = players_df['B/T'].apply(lambda x: x.replace('-', '/'))
    players_df['B/T'] = players_df['B/T'].apply(lambda x: x.replace('B', 'S')) # standaridzie both hands into switch hands
    
    players_df[['Batting', 'Throwing']] = players_df['B/T'].str.split('/', expand=True)

    return players_df

def clean_role_list(roles):
    if "Head Coach" in roles:
        roles = [r for r in roles if r not in ["Associate Head Coach", "Assistant Coach"]]
    elif "Associate Head Coach" in roles:
        # Remove Assistant Coach
        roles = [r for r in roles if r != "Assistant Coach"]
    
    if "Student Assistant Coach" in roles and "Assistant Coach" in roles:
        roles.remove("Student Assistant Coach")

    # Remove redundant variants
    if "Pitching Coach" in roles and "Pitching" in roles:
        roles.remove("Pitching")
    elif "Pitching" in roles:
        roles.append("Pitching Coach")
        roles.remove("Pitching")

    if "Hitting Coach" in roles and "Hitting" in roles:
        roles.remove("Hitting")
    elif "Hitting" in roles:
        roles.append("Hitting Coach")
        roles.remove("Hitting")

    if "Outfield" in roles:
        roles.append("Outfield Coach")
        roles.remove("Outfield")

    if "Infield" in roles and "Infield Coach" not in roles:
        roles.append("Infield Coach")
        roles.remove("Infield")

    # Avoid situations like: "Strength & Conditioning Coach" + "Strength" + "Conditioning"
    if "Strength" in roles and "Strength & Conditioning Coach" not in roles:
        roles.append("Strength & Conditioning Coach")
    if "Strength & Conditioning Coach" in roles:
        for r in ["Strength", "Conditioning"]:
            if r in roles:
                roles.remove(r)
    
    if "Recruiting" in roles:
        roles.remove("Recruiting")

    return roles

def extract_roles(title):

    ROLE_KEYWORDS = [
        # Primary roles
        "head coach",
        "associate head coach",
        "assistant coach",
        "pitching coach",
        "hitting coach",
        "recruiting coordinator",
        "strength and conditioning coach",
        "strength & conditioning coach",
        "strength coach",
        "volunteer coach",
        "student assistant coach",
        "student coach",
        "undergraduate assistant coach",
        "special assistant",
    ]

    CANONICAL_MAP = {
        "strength coach": "Strength & Conditioning Coach",
        "strength & conditioning coach": "Strength & Conditioning Coach",
        "strength and conditioning coach": "Strength & Conditioning Coach",
        "assistant coach": "Assistant Coach",
        "associate head coach": "Associate Head Coach",
        "head coach": "Head Coach",
        "volunteer coach": "Volunteer Coach",
        "student assistant coach": "Student Assistant Coach",
        "student coach": "Student Assistant Coach",
        "undergraduate assistant coach": "Student Assistant Coach",
        "special assistant": "Assistant Coach",
    }
    title = title.lower().strip()

    # Fix merged words
    title = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', title)

    # Normalize characters
    title = title.replace("&", "/")
    title = title.replace("/", " / ")
    title = re.sub(r"[(),]", " ", title)
    title = re.sub(r"\s+", " ", title)

    parts = [p.strip() for p in title.split("/") if p.strip()]

    roles = []

    for p in parts:
        for key in ROLE_KEYWORDS:
            if key in p:
                canonical = CANONICAL_MAP.get(key, key.title())
                if canonical not in roles:
                    roles.append(canonical)

    # Fallback: if no match found
    if not roles:
        roles.append("Assistant Coach")  # default

    roles = clean_role_list(roles)

    return roles

if __name__ == '__main__':
    players_df = pd.read_csv('data/raw/players.csv')
    lastschools_df = pd.read_csv('data/raw/lastschools.csv')
    coaches_df = pd.read_csv('data/raw/coaches.csv')

    players_df, lastschools_df = dedup_last_schools(players_df, lastschools_df)
    players_df = standardize_player_positions(players_df)
    players_df = standardize_batting_throwing(players_df)
    players_df = standardize_class_year(players_df)
    players_df, coaches_df = map_team(players_df, coaches_df)
    coaches_df = dedup_coaches(coaches_df)
    coaches_df['Role List'] = coaches_df['Title'].apply(extract_roles)

    players_df.to_csv('data/processed/players.csv', index=False)
    lastschools_df.to_csv('data/processed/lastschools.csv', index=False)
    coaches_df.to_csv('data/processed/coaches.csv', index=False)