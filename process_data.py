from rapidfuzz import fuzz, process

def dedup_high_schools(players_df, schools_df):
    canonical_hs = schools_df[schools_df['school type'] == 'high school']['name'].unique()

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
    schools_df['name'] = schools_df['name'].map(hs_mapping).fillna(schools_df['name'])
    
    schools_df.drop_duplicates(subset=['name'], inplace=True)

    print(f"Original: {len(hs_names)}")
    print(f"Canonical: {len(set(hs_mapping.values()))}")
    print(f"After deduplication: {len(hs_names) - len(set(hs_mapping.values()))} duplicates removed.")

    return players_df, schools_df

def dedup_coaches(coaches_df):
    # 1. John Smith at ABC Univ: Head Coach
    # 2. John Smith at ABC Univ: Recruiting Coordinator
    # => 1. John Smith at ABC Univ: Head Coach | Recruiting Coordinator
    coaches_clean = coaches_df.groupby(['Name', 'School'], as_index=False).agg({
        'Title': lambda x: ' | '.join(sorted(set(x)))
    })
    return coaches_clean