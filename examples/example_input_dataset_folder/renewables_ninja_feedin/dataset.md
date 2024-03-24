# EE-Einspeisezeitreihen

Einspeisezeitreihen für Erneuerbare Energien, normiert auf 1 MW bzw. 1 p.u.
Als Wetterjahr wird 2011 verwendet, siehe
[Szenarien](../../../../docs/sections/scenarios.md).

## Windenergie

Stündlich aufgelöste Zeitreihe der Windenergie Einspeisung über 1 Jahr auf Basis
von [MaStR](../bnetza_mastr/dataset.md) und
[renewables.ninja](http://renewables.ninja).
Auf einen Auflösung auf Gemeindeebene wird verzichtet, da die Differenz der
Produktion der Gemeinden nach renewables.ninja <5 % beträgt.

### Windenergieanlage (2022)

Für renewables.ninja sind Position (lat, lon), Nennleistung (capacity),
Nabenhöhe und Turbinentyp erforderlich.

#### Position

Hierfür wird aus den Zentroiden der Gemeinden ein räumlicher Mittelwert
anhand des Datensatzes
[bkg_vg250_muns_region](../../datasets/bkg_vg250_muns_region/dataset.md)
(`bkg_vg250_muns_region.gpkg`) gebildet:

```
import geopandas as gpd
import os.path

def get_position(gdf):
    df = gpd.read_file(gdf)
    points_of_muns = df["geometry"].centroid
    points_of_muns_crs = points_of_muns.to_crs(4326)
    point_df = [
        points_of_muns_crs.y.sum()/len(points_of_muns),
        points_of_muns_crs.x.sum()/len(points_of_muns)
    ]
    return point_df

data_folder = os.path.join("your_data_folder")
muns_gpkg = os.path.join(data_folder, "bkg_vg250_muns_region.gpkg")
center_position = get_position(muns_gpkg)
```

#### Nennleistung

Wird auf 1 MW gesetzt/normiert.

#### Nabenhöhe

Aus dem Datensatz
[bnetza_mastr_wind_region](../../datasets/bnetza_mastr_wind_region/dataset.md)
(`bnetza_mastr_wind_agg_abw.gpkg`) wird ein Mittelwer von 100 m abgeleitet.

```
import geopandas as gpd

df = gpd.read_file("bnetza_mastr_wind_agg_abw.gpkg")
height = df[["hub_height"]].mean()
```

#### Turbinentyp

Annahme: Innerhalb eines Herstellers sind Leistungskurven sehr ähnlich.
Daher werden zwei größten Hersteller mit jeweiligen häufigsten Turbinentyp
ausgewählt - diese sind Enercon und Vestas mit ca. 70 % und ca. 30%.

```
import geopandas as gpd

df = gpd.read_file("bnetza_mastr_wind_agg_abw.gpkg")
manufacturers = df[
    ["manufacturer_name", "status"]
].groupby("manufacturer_name").count().sort_values(
    by="status", ascending=False
)
```

Häufigste Turbinentypen sind *Enercon E-70* und *Vestas V80*. Daher werden
*Enercon E70 2000* und *Vestas V80 2000* in renewables.ninja ausgewählt.

```
man_1 = manufacturers.index[0]
man_2 = manufacturers.index[1]

type_1 = df[
    ["manufacturer_name", "type_name", "status"]
].where(df["manufacturer_name"] == man_1).groupby(
    "type_name").count().sort_values(by="status", ascending=False)

type_2 = df[
    ["manufacturer_name", "type_name", "status"]
].where(df["manufacturer_name"] == man_2).groupby(
    "type_name").count().sort_values(by="status", ascending=False)
```

### Raw Data von [renewables.ninja](http://renewables.ninja) API

Es werden zwei Zeitreihen für oben beschriebenen Vergleichsanlagen berechnet:

```
import json
import requests
import pandas as pd
import geopandas as gpd

def change_wpt(position, capacity, height, turbine):
    args = {
        'lat': 51.8000,  # 51.5000-52.0000
        'lon': 12.2000,  # 11.8000-13.1500
        'date_from': '2011-01-01',
        'date_to': '2011-12-31',
        'capacity': 1000.0,
        'height': 100,
        'turbine': 'Vestas V164 7000',
        'format': 'json',
        'local_time': 'true',
        'raw': 'false',
    }

    args['capacity'] = capacity
    args['height'] = height
    args['lat'] = position[0]
    args['lon'] = position[1]
    args['turbine'] = turbine

    return args

def get_df(args):
    token = 'Please get your own'
    api_base = 'https://www.renewables.ninja/api/'

    s = requests.session()
    # Send token header with each request
    s.headers = {'Authorization': 'Token ' + token}

    url = api_base + 'data/wind'

    r = s.get(url, params=args)

    parsed_response = json.loads(r.text)
    df = pd.read_json(
    json.dumps(parsed_response['data']),orient='index')
    metadata = parsed_response['metadata']
    return df

enercon_production = get_df(change_wpt(
    position,
    capacity=1,
    height=df[["hub_height"]].mean(),
    turbine=enercon)
)

vestas_production = get_df(change_wpt(
    position,
    capacity=1000,
    height=df[["hub_height"]].mean(),
    turbine=vestas)
)
```

### Gewichtung und Skalierung der Zeitreihen

Um die Charakteristika der beiden o.g. Anlagentypen zu berücksichtigen, erfolgt
eine gewichtete Summierung der Zeitreihen anhand der berechneten Häufigkeit.

### Zukunftsszenarien

Analog zu dem oben beschriebenen Vorgehen wird eine separate Zeitreihe für
zukünftige WEA berechnet. Hierbei wird eine Enercon E126 6500 mit einer
Nabenhöhe von 159 m angenommen
([PV- und Windflächenrechner](https://zenodo.org/record/6794558)).

Da die Zeitreihe sich nur marginal von der obigen Status-quo-Zeitreihe
unterscheidet, wird letztere sowohl für den Status quo als auch die
Zukunftsszenarien verwendet.

- Einspeisezeitreihe: `wind_feedin_timeseries.csv`

## Freiflächen-Photovoltaik

### PV-Anlage (2022)

Stündlich aufgelöste Zeitreihe der Photovoltaikeinspeisung über 1 Jahr auf Basis
von [MaStR](../bnetza_mastr/dataset.md) und
[renewables.ninja](http://renewables.ninja).
Wie bei der Windeinspeisung wird auf eine Auflsöung auf Gemeindeebene aufgrund
geringer regionaler Abweichungen verzichtet.

Für die Generierung der Zeitreihe über
[renewables.ninja](http://renewables.ninja)
wird eine Position(lat, lon), Nennleistung (capacity), Verluste (system_loss)
Nachführung (tracking), Neigung (tilt) und der Azimutwinkel (azim) benötigt.

Als Position wird analog zur Windenergieanlage der räumlicher Mittelwert
verwendet. Laut MaStR werden lediglich 13 Anlagen nachgeführt (0,01 % der
Kapazität), die Nachführung wird daher vernachlässigt. Die Neigung ist aus MaStR
nicht bekannt, es dominieren jedoch Anlagen auf Freiflächen sowie Flachdächern
im landwirtschaftlichen Kontext. Nach
[Ariadne Szenarienreport](https://ariadneprojekt.de/media/2022/02/Ariadne_Szenarienreport_Oktober2021_corr0222_lowres.pdf)
wird diese mit 30° angenommen.
Die Nennleistung Wird auf 1 MW gesetzt/normiert.

### Zukunftsszenarien

Die Status-quo-Zeitreihe wird sowohl für den Status quo als auch die
Zukunftsszenarien verwendet.

- Einspeisezeitreihe: `pv_feedin_timeseries.csv`

## Solarthermie

- Einspeisezeitreihe: `st_feedin_timeseries.csv` (Kopie von
  PV-Einspeisezeitreihe)

## Laufwasserkraft

Hier wird eine konstante Einspeisung angenommen.

- Einspeisezeitreihe: `ror_feedin_timeseries.csv`
