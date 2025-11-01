####### OBJECTIFS ########
# Nous cherchons ici à obtenir un dataset comportant les vacances et jour férié pour paris, sur les dernières années
# Les données proviennent de https://data.education.gouv.fr/explore/dataset/fr-en-calendrier-scolaire/table/?disjunctive.zones&disjunctive.description&disjunctive.location&disjunctive.annee_scolaire&disjunctive.population&sort=end_date
##########################

import pandas as pd
import re
from datetime import datetime
import holidays

df = pd.read_csv('trafic_prediction/dataset_brut/fr-en-calendrier-scolaire.csv', sep=";")

df = df[(df['Académies'] == "Paris") & (df['annee_scolaire'].isin(['2023-2024','2024-2025','2025-2026']))].reset_index(drop=True)

df.drop(df[df['Population'] == 'Enseignants'].index, inplace=True)



# Convertir en UTC puis enlever le timezone (recommandé pour éviter les ambiguïtés)
for col in ['Date de début', 'Date de fin']:

    df[col] = pd.to_datetime(df[col], errors='coerce')  # parse même avec timezone

    # Supprime uniquement le fuseau horaire sans changer l'heure

    df[col] = df[col].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)


def parse_school_year_label(label):
    m = re.search(r"(\d{4})[-_](\d{4})", str(label))
    if not m:
        raise ValueError(f"Format d'année scolaire inattendu: {label}")
    y1, y2 = int(m.group(1)), int(m.group(2))
    return y1, y2

def school_year_for_date(d, sep):
    y = d.year
    if d.month >= 9:
        return f"{y}{sep}{y+1}"
    else:
        return f"{y-1}{sep}{y}"

years = set()
for label in df['annee_scolaire'].dropna().unique():
    y1, y2 = parse_school_year_label(label)
    years.add(y1)
    years.add(y2)

years_needed = sorted(years | {min(years)-1, max(years)+1})

fr_h = holidays.France(years=years_needed)  
records = []
for d, name in sorted(fr_h.items()):
    d_ts = pd.Timestamp(d)
    # ✅ NORMALISER ET S'ASSURER QU'IL N'Y A PAS DE TIMEZONE
    d_normalized = d_ts.normalize().tz_localize(None) if d_ts.tz else d_ts.normalize()
    
    records.append({
        "date": d_normalized,
        "Description": str(name),                     
        "annee_scolaire": school_year_for_date(d_ts, sep="-"),
        "Zones": "Zone C",                          
        "Population": "-", 
        "Académies": "Paris",                                                
        "Date de début": d_normalized,           
        "Date de fin": d_normalized              
    })

feries_df = pd.DataFrame.from_records(records).drop_duplicates(subset=["date", "Description"])

common_cols = [c for c in feries_df.columns if c in df.columns]
if not common_cols:
    cols_min = ["annee_scolaire", "Zones"]
    for c in cols_min:
        if c not in feries_df.columns:
            feries_df[c] = None
    common_cols = [c for c in feries_df.columns if c in df.columns]

df_plus = pd.concat([df, feries_df[common_cols]], ignore_index=True)

print(df_plus.head(50))

# ✅ VÉRIFIER QUE LES DATES SONT BIEN SANS TIMEZONE
print("\n✅ Types des colonnes de dates:")
print(f"Date de début: {df_plus['Date de début'].dtype}")
print(f"Date de fin: {df_plus['Date de fin'].dtype}")
df_plus = df_plus[(df_plus['Date de début'] >= '2024-01-01') & (df_plus['Date de début'] <'2026-01-01')].reset_index(drop=True)

df_plus.columns = df_plus.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
df_plus.drop(columns=['population','zones','annee_scolaire','academies'], inplace=True, errors='ignore')

# ✅ SAUVEGARDER AVEC date_format POUR GARANTIR LE FORMAT
df_plus.to_csv('trafic_prediction/dataset/vacances.csv', sep=';', index=False, date_format='%Y-%m-%d %H:%M:%S')

print("\n✅ Fichier sauvegardé avec dates uniformisées!")