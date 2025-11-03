import pandas as pd

vacances = pd.read_csv(
    "dataset/vacances.csv",
    parse_dates=["Date de début", "Date de fin"],
    sep=";",
    index_col=0,
)

feries = vacances.iloc[18:]
feries["Date de début"] = pd.to_datetime(feries["Date de début"])
feries["Date de fin"] = pd.to_datetime(feries["Date de fin"]) + pd.Timedelta(days=1)


vacances = vacances.iloc[:18]  # keep only metropolitan France
vacances["Date de début"] = pd.to_datetime(vacances["Date de début"], utc=True)
vacances["Date de fin"] = pd.to_datetime(vacances["Date de fin"], utc=True)


paris_respire = pd.read_csv("dataset/paris_respire.csv", parse_dates=["date"], sep=",")[
    ["date"]
]
paris_respire["date"] = pd.to_datetime(paris_respire["date"])


def add_off_days_columns(df):
    df["is_holiday"] = (
        df["Date et heure de comptage"]
        .apply(
            lambda x: any(
                (x >= row["Date de début"]) and (x < row["Date de fin"])
                for _, row in vacances.iterrows()
            )
        )
        .astype(int)
    )
    df["is_off_day"] = (
        df["Date et heure de comptage"]
        .apply(
            lambda x: any(
                (x.date() >= row["Date de début"].date())
                and (x.date() < row["Date de fin"].date())
                for _, row in feries.iterrows()
            )
        )
        .astype(int)
    )
    return df


def add_paris_respire_column(df, paris_respire):
    df["is_paris_respire"] = df["Date et heure de comptage"].apply(
        lambda x: int(
            x.date() in pd.to_datetime(paris_respire["date"]).dt.date.values
            and 10 <= x.hour <= 18
        )
    )
    return df


def add_categorical_day_columns(df):
    df = add_off_days_columns(df)
    df = add_paris_respire_column(df, paris_respire)
    return df
