from rapidfuzz import fuzz, process
import re
import pandas as pd

def dedup_high_schools(players_df, highschools_df):
    canonical_hs = highschools_df['name'].unique()

    hs_names = players_df['High School'].unique()

    hs_mapping = {}

    for school in hs_names:
        matches = process.extract(
            school,
            canonical_hs,
            scorer=fuzz.partial_ratio,
            score_cutoff=70,
        )
        if matches:
            hs_mapping[school] = matches[0][0]
        else:
            hs_mapping[school] = school

    players_df['High School'] = players_df['High School'].map(hs_mapping).fillna(players_df['High School'])
    highschools_df['name'] = highschools_df['name'].map(hs_mapping).fillna(highschools_df['name'])
    
    highschools_df.drop_duplicates(subset=['name'], inplace=True)

    print(f"Original: {len(hs_names)}")
    print(f"Canonical: {len(set(hs_mapping.values()))}")
    print(f"After deduplication: {len(hs_names) - len(set(hs_mapping.values()))} duplicates removed.")

    return players_df, highschools_df

def dedup_coaches(coaches_df):
    # 1. John Smith at ABC Univ: Head Coach
    # 2. John Smith at ABC Univ: Recruiting Coordinator
    # => 1. John Smith at ABC Univ: Head Coach | Recruiting Coordinator
    coaches_clean = coaches_df.groupby(['Name', 'College', 'Season'], as_index=False).agg({
        'Title': lambda x: ' | '.join(sorted(set(x)))
    })
    return coaches_clean

def extract_positions(pos_str, mapping):
    parts = re.split(r'[/,|]', pos_str)
    return [mapping.get(p.strip()) for p in parts if p.strip() and mapping.get(p.strip())]

def standardize_player_positions(players_df):
    mapping = {
        "OF": "Outfielder",
        "INF": "Infielder",
        "IF": "Infielder",
        "Inf.": "Infielder",
        "C": "Catcher",
        "RHP": "Right-Handed Pitcher",
        "LHP": "Left-Handed Pitcher",
        "P": "Pitcher",
        "UTIL": "Utility",
        "UTL": "Utility",
        "UT": "Utility",
        "1B": "First Base",
        "2B": "Second Base",
        "3B": "Third Base",
        "SS": "Shortstop",
        "CF": "Center Field",
        "DH": "Designated Hitter",
        "Infield": "Infielder",
        "Outfield": "Outfielder",
        "Catcher": "Catcher",
        "Catcher/Infield": "Catcher",
        "Infield/Outfield": "Infielder",
    }

    players_df['Position List'] = players_df['Position'].apply(lambda x: extract_positions(str(x), mapping))
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
        'Sr.+': 'Senior',
        'So.': 'Sophomore',
        'R-Fr.': 'Redshirt Freshman',
        'Fr.': 'Freshman',
        'R-Jr.': 'Redshirt Junior',
        'R-So.': 'Redshirt Sophomore',
        'Gr.': 'Graduate',
        'Gr.+': 'Graduate',
        'R-Sr.': 'Redshirt Senior',
        '5th': 'Fifth Year',
    }

    players_df['Class Year'] = players_df['Class Year'].apply(lambda x: mapping.get(str(x).strip(), x))
    
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
    
    def handle_bt_na(bt):
        if 'Year' in bt:
            return 'N/A'
        return bt
    players_df['B/T'] = players_df['B/T'].apply(handle_bt_na)

    # Map single-letter to words
    code_map = {
        'L': 'Left',
        'R': 'Right',
        'S': 'Switch',   # keep switch if it exists
        'B': 'Switch',   # keep both as switch as well
    }

    def parse_bt(bt):
        val = str(bt).strip().upper().replace('-', '/')
        parts = [p for p in val.split('/') if p]
        bat = code_map.get(parts[0], None) if parts else None
        thr = code_map.get(parts[1], None) if len(parts) > 1 else None
        return pd.Series([bat, thr])

    players_df[['Batting', 'Throwing']] = players_df['B/T'].apply(parse_bt)
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

def standardize_hometown(players_df):
    # Mapping for US States and Canadian Provinces commonly found in baseball rosters
    state_map = {
        # US States
        'Alabama': 'AL', 'Ala.': 'AL', 'Ala': 'AL',
        'Alaska': 'AK', 'Alas.': 'AK',
        'Arizona': 'AZ', 'Ariz.': 'AZ', 'Ariz': 'AZ',
        'Arkansas': 'AR', 'Ark.': 'AR',
        'California': 'CA', 'Calif.': 'CA', 'Calif': 'CA', 'Cal.': 'CA', 'Calilf.': 'CA',
        'Colorado': 'CO', 'Colo.': 'CO',
        'Connecticut': 'CT', 'Conn.': 'CT',
        'Delaware': 'DE', 'Del.': 'DE',
        'Florida': 'FL', 'Fla.': 'FL', 'Fla': 'FL',
        'Georgia': 'GA', 'Ga.': 'GA',
        'Hawaii': 'HI', 'Hawai\'i': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL', 'Ill.': 'IL',
        'Indiana': 'IN', 'Ind.': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS', 'Kan.': 'KS',
        'Kentucky': 'KY', 'Ky.': 'KY',
        'Louisiana': 'LA', 'La.': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD', 'Md.': 'MD',
        'Massachusetts': 'MA', 'Mass.': 'MA',
        'Michigan': 'MI', 'Mich.': 'MI',
        'Minnesota': 'MN', 'Minn.': 'MN',
        'Mississippi': 'MS', 'Miss.': 'MS',
        'Missouri': 'MO', 'Mo.': 'MO',
        'Montana': 'MT', 'Mont.': 'MT',
        'Nebraska': 'NE', 'Neb.': 'NE',
        'Nevada': 'NV', 'Nev.': 'NV',
        'New Hampshire': 'NH', 'N.H.': 'NH',
        'New Jersey': 'NJ', 'N.J.': 'NJ',
        'New Mexico': 'NM', 'N.M.': 'NM',
        'New York': 'NY', 'N.Y.': 'NY',
        'North Carolina': 'NC', 'N.C.': 'NC',
        'North Dakota': 'ND', 'N.D.': 'ND',
        'Ohio': 'OH',
        'Oklahoma': 'OK', 'Okla.': 'OK',
        'Oregon': 'OR', 'Ore.': 'OR',
        'Pennsylvania': 'PA', 'Pa.': 'PA', 'Penn.': 'PA',
        'Rhode Island': 'RI', 'R.I.': 'RI',
        'South Carolina': 'SC', 'S.C.': 'SC',
        'South Dakota': 'SD', 'S.D.': 'SD',
        'Tennessee': 'TN', 'Tenn.': 'TN',
        'Texas': 'TX', 'Tex.': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT', 'Vt.': 'VT',
        'Virginia': 'VA', 'Va.': 'VA',
        'Washington': 'WA', 'Wash.': 'WA',
        'West Virginia': 'WV', 'W.Va.': 'WV',
        'Wisconsin': 'WI', 'Wis.': 'WI', 'Wisc.': 'WI',
        'Wyoming': 'WY', 'Wyo.': 'WY',
        'District of Columbia': 'DC', 'D.C.': 'DC',
        
        # Common International/Territories
        'Puerto Rico': 'PR', 'P.R.': 'PR',
        'Ontario': 'ON',
        'British Columbia': 'BC', 'B.C.': 'BC',
        'Alberta': 'AB',
        'Quebec': 'QC',
    }

    def clean_hometown(val):
        if pd.isna(val):
            return "Unknown"
        
        val = str(val).strip()
        
        # Split by comma to separate City and State
        parts = val.split(',')
        
        if len(parts) >= 2:
            city = parts[0].strip().title()
            # Take the last part as the state/country
            state_raw = parts[-1].strip()
            
            # Check mapping (case-insensitive check)
            # try exact match first, then title case
            state_clean = state_map.get(state_raw)
            if not state_clean:
                state_clean = state_map.get(state_raw.title())
            
            # If still not found, keep original but strip periods if it looks like an abbrev
            if not state_clean:
                state_clean = state_raw
            
            return f"{city}, {state_clean}"
            
        return val.title()

    players_df['Hometown'] = players_df['Hometown'].apply(clean_hometown)
    return players_df

if __name__ == '__main__':
    players_df = pd.read_csv('data/raw/players.csv')
    highschools_df = pd.read_csv('data/raw/highschools.csv')
    coaches_df = pd.read_csv('data/raw/coaches.csv')

    players_df, highschools_df = dedup_high_schools(players_df, highschools_df)
    players_df = standardize_player_positions(players_df)
    players_df = standardize_batting_throwing(players_df)
    players_df = standardize_class_year(players_df)
    players_df = standardize_hometown(players_df)
    coaches_df = dedup_coaches(coaches_df)
    players_df, coaches_df = map_team(players_df, coaches_df)
    coaches_df['Role List'] = coaches_df['Title'].apply(extract_roles)

    players_df.to_csv('data/processed/players.csv', index=False)
    highschools_df.to_csv('data/processed/highschools.csv', index=False)
    coaches_df.to_csv('data/processed/coaches.csv', index=False)