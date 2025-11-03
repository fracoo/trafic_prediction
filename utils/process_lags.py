import pandas as pd


def process_lags(df, N_day_lags):
    df["Date et heure de comptage"] = pd.to_datetime(
        df["Date et heure de comptage"], utc=True
    )
    df = df.sort_values("Date et heure de comptage").set_index(
        "Date et heure de comptage"
    )

    # 1 - Reconstituer toutes les heures manquantes
    full_index = pd.date_range(df.index.min(), df.index.max(), freq="H")
    df = df.reindex(full_index)

    # 2 - Créer les lags
    Lags = []
    for lag_day in range(1, N_day_lags + 1):
        for lag_hour in range(24):
            lag_name = f"lag_{lag_day}_{lag_hour}"
            df[lag_name] = df["Débit horaire"].shift(lag_day * 24 + lag_hour)
            Lags.append(lag_name)

    # 3 - Remettre les dates initiales (optionnel)
    df = df.reset_index().rename(columns={"index": "Date et heure de comptage"})
    return df, Lags
