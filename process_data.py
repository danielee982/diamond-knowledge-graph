from rapidfuzz import fuzz, process
import re

def dedup_high_schools(players_df, highschools_df):
    # canonical_hs = schools_df[schools_df['school type'] == 'high school']['name'].unique()
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
    coaches_clean = coaches_df.groupby(['Name', 'School'], as_index=False).agg({
        'Title': lambda x: ' | '.join(sorted(set(x)))
    })
    return coaches_clean

def extract_positions(pos_str, mapping):
    parts = re.split(r'[/,|]', pos_str)
    return [mapping.get(p) for p in parts if p]

def standardize_positions(players_df):
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