#!/usr/bin/env python3
"""
Seed script for countries table
"""

from sqlalchemy.orm import Session

from app.models.country import Country

COUNTRIES_DATA = [
    {
        "code": "USA",
        "name": "United States of America",
        "lat": 39.8283,
        "lng": -98.5795,
    },
    {
        "code": "CAN",
        "name": "Canada",
        "lat": 56.1304,
        "lng": -106.3468,
    },
    {
        "code": "MEX",
        "name": "Mexico",
        "lat": 23.6345,
        "lng": -102.5528,
    },
    {
        "code": "GBR",
        "name": "United Kingdom",
        "lat": 55.3781,
        "lng": -3.4360,
    },
    {
        "code": "DEU",
        "name": "Germany",
        "lat": 51.1657,
        "lng": 10.4515,
    },
    {
        "code": "FRA",
        "name": "France",
        "lat": 46.2276,
        "lng": 2.2137,
    },
    {
        "code": "ESP",
        "name": "Spain",
        "lat": 40.4637,
        "lng": -3.7492,
    },
    {
        "code": "NLD",
        "name": "Netherlands",
        "lat": 52.1326,
        "lng": 5.2913,
    },
    {
        "code": "DNK",
        "name": "Denmark",
        "lat": 56.2639,
        "lng": 9.5018,
    },
    {
        "code": "NOR",
        "name": "Norway",
        "lat": 60.4720,
        "lng": 8.4689,
    },
    {
        "code": "SWE",
        "name": "Sweden",
        "lat": 60.1282,
        "lng": 18.6435,
    },
    {
        "code": "FIN",
        "name": "Finland",
        "lat": 61.9241,
        "lng": 25.7482,
    },
    {
        "code": "POL",
        "name": "Poland",
        "lat": 51.9194,
        "lng": 19.1451,
    },
    {
        "code": "BEL",
        "name": "Belgium",
        "lat": 50.5039,
        "lng": 4.4699,
    },
    {
        "code": "AUT",
        "name": "Austria",
        "lat": 47.5162,
        "lng": 14.5501,
    },
    {
        "code": "CHE",
        "name": "Switzerland",
        "lat": 46.8182,
        "lng": 8.2275,
    },
    {
        "code": "ITA",
        "name": "Italy",
        "lat": 41.8719,
        "lng": 12.5674,
    },
    {
        "code": "PRT",
        "name": "Portugal",
        "lat": 39.3999,
        "lng": -8.2245,
    },
    {
        "code": "IRL",
        "name": "Ireland",
        "lat": 53.4129,
        "lng": -8.2439,
    },
    {
        "code": "AUS",
        "name": "Australia",
        "lat": -25.2744,
        "lng": 133.7751,
    },
    {
        "code": "NZL",
        "name": "New Zealand",
        "lat": -40.9006,
        "lng": 174.8860,
    },
    {
        "code": "JPN",
        "name": "Japan",
        "lat": 36.2048,
        "lng": 138.2529,
    },
    {
        "code": "KOR",
        "name": "South Korea",
        "lat": 35.9078,
        "lng": 127.7669,
    },
    {
        "code": "CHN",
        "name": "China",
        "lat": 35.8617,
        "lng": 104.1954,
    },
    {
        "code": "IND",
        "name": "India",
        "lat": 20.5937,
        "lng": 78.9629,
    },
    {
        "code": "BRA",
        "name": "Brazil",
        "lat": -14.2350,
        "lng": -51.9253,
    },
    {
        "code": "ARG",
        "name": "Argentina",
        "lat": -38.4161,
        "lng": -63.6167,
    },
    {
        "code": "CHL",
        "name": "Chile",
        "lat": -35.6751,
        "lng": -71.5430,
    },
    {
        "code": "ZAF",
        "name": "South Africa",
        "lat": -30.5595,
        "lng": 22.9375,
    },
    # Additional European/Energy Market Countries
    {
        "code": "EST",
        "name": "Estonia",
        "lat": 58.5953,
        "lng": 25.0136,
    },
    {
        "code": "LTU",
        "name": "Lithuania",
        "lat": 55.1694,
        "lng": 23.8813,
    },
    {
        "code": "LVA",
        "name": "Latvia",
        "lat": 56.8796,
        "lng": 24.6032,
    },
    {
        "code": "RUS",
        "name": "Russia",
        "lat": 61.5240,
        "lng": 105.3188,
    },
    {
        "code": "BLR",
        "name": "Belarus",
        "lat": 53.7098,
        "lng": 27.9534,
    },
    {
        "code": "UKR",
        "name": "Ukraine",
        "lat": 48.3794,
        "lng": 31.1656,
    },
    {
        "code": "GRC",
        "name": "Greece",
        "lat": 39.0742,
        "lng": 21.8243,
    },
    {
        "code": "HRV",
        "name": "Croatia",
        "lat": 45.1000,
        "lng": 15.2000,
    },
    {
        "code": "HUN",
        "name": "Hungary",
        "lat": 47.1625,
        "lng": 19.5033,
    },
    {
        "code": "MKD",
        "name": "North Macedonia",
        "lat": 41.6086,
        "lng": 21.7453,
    },
    {
        "code": "ROU",
        "name": "Romania",
        "lat": 45.9432,
        "lng": 24.9668,
    },
    {
        "code": "SVN",
        "name": "Slovenia",
        "lat": 46.1512,
        "lng": 14.9955,
    },
    {
        "code": "SVK",
        "name": "Slovakia",
        "lat": 48.6690,
        "lng": 19.6990,
    },
    {
        "code": "CZE",
        "name": "Czech Republic",
        "lat": 49.8175,
        "lng": 15.4730,
    },
    {
        "code": "TUR",
        "name": "Turkey",
        "lat": 38.9637,
        "lng": 35.2433,
    },
    {
        "code": "MLT",
        "name": "Malta",
        "lat": 35.8997,
        "lng": 14.5146,
    },
    {
        "code": "ALB",
        "name": "Albania",
        "lat": 41.1533,
        "lng": 20.1683,
    },
    {
        "code": "BIH",
        "name": "Bosnia and Herzegovina",
        "lat": 43.9159,
        "lng": 17.6791,
    },
    {
        "code": "BGR",
        "name": "Bulgaria",
        "lat": 42.7339,
        "lng": 25.4858,
    },
    {
        "code": "MNE",
        "name": "Montenegro",
        "lat": 42.7087,
        "lng": 19.3744,
    },
    {
        "code": "SRB",
        "name": "Serbia",
        "lat": 44.0165,
        "lng": 21.0059,
    },
    {
        "code": "CYP",
        "name": "Cyprus",
        "lat": 35.1264,
        "lng": 33.4299,
    },
    {
        "code": "MDA",
        "name": "Moldova",
        "lat": 47.4116,
        "lng": 28.3699,
    },
    {
        "code": "ARM",
        "name": "Armenia",
        "lat": 40.0691,
        "lng": 45.0382,
    },
    {
        "code": "GEO",
        "name": "Georgia",
        "lat": 42.3154,
        "lng": 43.3569,
    },
    {
        "code": "AZE",
        "name": "Azerbaijan",
        "lat": 40.1431,
        "lng": 47.5769,
    },
    {
        "code": "XKX",
        "name": "Kosovo",
        "lat": 42.6026,
        "lng": 20.9030,
    },
    {
        "code": "LUX",
        "name": "Luxembourg",
        "lat": 49.8153,
        "lng": 6.1296,
    },
]


def seed_countries(db: Session):
    """Seed countries table with initial data"""
    print(f"  Checking for existing countries...")

    # Get existing country codes
    existing_codes = {c.code for c in db.query(Country.code).all()}

    # Filter out countries that already exist
    countries_to_add = []
    for country_data in COUNTRIES_DATA:
        if country_data["code"] not in existing_codes:
            countries_to_add.append(Country(**country_data))

    if not countries_to_add:
        print(f"  Found {len(existing_codes)} existing countries, no new countries to add")
        return

    print(f"  Adding {len(countries_to_add)} new countries...")

    db.add_all(countries_to_add)
    db.commit()

    print(f"  Successfully added {len(countries_to_add)} countries")
