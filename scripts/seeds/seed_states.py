#!/usr/bin/env python3
"""
Seed script for states table
"""

from sqlalchemy.orm import Session

from app.models.country import Country
from app.models.state import State

STATES_DATA = [
    # US States (major ones with significant energy infrastructure)
    {"code": "AL", "name": "Alabama", "country_code": "USA", "lat": 32.3617, "lng": -86.7916},
    {"code": "AK", "name": "Alaska", "country_code": "USA", "lat": 64.0685, "lng": -152.2782},
    {"code": "AZ", "name": "Arizona", "country_code": "USA", "lat": 34.2744, "lng": -111.2847},
    {"code": "AR", "name": "Arkansas", "country_code": "USA", "lat": 34.7519, "lng": -92.1313},
    {"code": "CA", "name": "California", "country_code": "USA", "lat": 36.7783, "lng": -119.4179},
    {"code": "CO", "name": "Colorado", "country_code": "USA", "lat": 39.5501, "lng": -105.7821},
    {"code": "CT", "name": "Connecticut", "country_code": "USA", "lat": 41.6032, "lng": -73.0877},
    {"code": "DE", "name": "Delaware", "country_code": "USA", "lat": 38.9108, "lng": -75.5277},
    {"code": "FL", "name": "Florida", "country_code": "USA", "lat": 27.7663, "lng": -81.6868},
    {"code": "GA", "name": "Georgia", "country_code": "USA", "lat": 32.1656, "lng": -82.9001},
    {"code": "HI", "name": "Hawaii", "country_code": "USA", "lat": 19.8968, "lng": -155.5828},
    {"code": "ID", "name": "Idaho", "country_code": "USA", "lat": 44.0682, "lng": -114.7420},
    {"code": "IL", "name": "Illinois", "country_code": "USA", "lat": 40.0417, "lng": -89.1965},
    {"code": "IN", "name": "Indiana", "country_code": "USA", "lat": 39.7817, "lng": -86.1478},
    {"code": "IA", "name": "Iowa", "country_code": "USA", "lat": 41.8780, "lng": -93.0977},
    {"code": "KS", "name": "Kansas", "country_code": "USA", "lat": 38.5266, "lng": -96.7265},
    {"code": "KY", "name": "Kentucky", "country_code": "USA", "lat": 37.8393, "lng": -84.2700},
    {"code": "LA", "name": "Louisiana", "country_code": "USA", "lat": 30.9843, "lng": -91.9623},
    {"code": "ME", "name": "Maine", "country_code": "USA", "lat": 45.2538, "lng": -69.4455},
    {"code": "MD", "name": "Maryland", "country_code": "USA", "lat": 39.0458, "lng": -76.6413},
    {"code": "MA", "name": "Massachusetts", "country_code": "USA", "lat": 42.4072, "lng": -71.3824},
    {"code": "MI", "name": "Michigan", "country_code": "USA", "lat": 44.3467, "lng": -85.4102},
    {"code": "MN", "name": "Minnesota", "country_code": "USA", "lat": 46.3287, "lng": -94.3053},
    {"code": "MS", "name": "Mississippi", "country_code": "USA", "lat": 32.3547, "lng": -89.3985},
    {"code": "MO", "name": "Missouri", "country_code": "USA", "lat": 37.9643, "lng": -91.8318},
    {"code": "MT", "name": "Montana", "country_code": "USA", "lat": 47.0527, "lng": -109.6333},
    {"code": "NE", "name": "Nebraska", "country_code": "USA", "lat": 41.4925, "lng": -99.9018},
    {"code": "NV", "name": "Nevada", "country_code": "USA", "lat": 38.8026, "lng": -116.4194},
    {"code": "NH", "name": "New Hampshire", "country_code": "USA", "lat": 43.1939, "lng": -71.5724},
    {"code": "NJ", "name": "New Jersey", "country_code": "USA", "lat": 40.0583, "lng": -74.4057},
    {"code": "NM", "name": "New Mexico", "country_code": "USA", "lat": 34.5199, "lng": -105.8701},
    {"code": "NY", "name": "New York", "country_code": "USA", "lat": 43.2994, "lng": -74.2179},
    {
        "code": "NC",
        "name": "North Carolina",
        "country_code": "USA",
        "lat": 35.7596,
        "lng": -79.0193,
    },
    {"code": "ND", "name": "North Dakota", "country_code": "USA", "lat": 47.5515, "lng": -101.0020},
    {"code": "OH", "name": "Ohio", "country_code": "USA", "lat": 40.4173, "lng": -82.9071},
    {"code": "OK", "name": "Oklahoma", "country_code": "USA", "lat": 35.0078, "lng": -97.0929},
    {"code": "OR", "name": "Oregon", "country_code": "USA", "lat": 43.8041, "lng": -120.5542},
    {"code": "PA", "name": "Pennsylvania", "country_code": "USA", "lat": 41.2033, "lng": -77.1945},
    {"code": "RI", "name": "Rhode Island", "country_code": "USA", "lat": 41.6809, "lng": -71.5118},
    {
        "code": "SC",
        "name": "South Carolina",
        "country_code": "USA",
        "lat": 33.8191,
        "lng": -80.9066,
    },
    {"code": "SD", "name": "South Dakota", "country_code": "USA", "lat": 44.2619, "lng": -100.3363},
    {"code": "TN", "name": "Tennessee", "country_code": "USA", "lat": 35.7478, "lng": -86.6923},
    {"code": "TX", "name": "Texas", "country_code": "USA", "lat": 31.0545, "lng": -97.5635},
    {"code": "UT", "name": "Utah", "country_code": "USA", "lat": 39.3210, "lng": -111.0937},
    {"code": "VT", "name": "Vermont", "country_code": "USA", "lat": 44.0459, "lng": -72.7107},
    {"code": "VA", "name": "Virginia", "country_code": "USA", "lat": 37.7693, "lng": -78.2057},
    {
        "code": "WA-US",
        "name": "Washington",
        "country_code": "USA",
        "lat": 47.7511,
        "lng": -120.7401,
    },
    {"code": "WV", "name": "West Virginia", "country_code": "USA", "lat": 38.4680, "lng": -80.9696},
    {"code": "WI", "name": "Wisconsin", "country_code": "USA", "lat": 44.2563, "lng": -89.6385},
    {"code": "WY", "name": "Wyoming", "country_code": "USA", "lat": 42.7475, "lng": -107.2085},
    # Canadian Provinces
    {"code": "AB", "name": "Alberta", "country_code": "CAN", "lat": 53.9333, "lng": -116.5765},
    {
        "code": "BC",
        "name": "British Columbia",
        "country_code": "CAN",
        "lat": 53.7267,
        "lng": -127.6476,
    },
    {"code": "MB", "name": "Manitoba", "country_code": "CAN", "lat": 53.7609, "lng": -98.8139},
    {"code": "NB", "name": "New Brunswick", "country_code": "CAN", "lat": 46.5653, "lng": -66.4619},
    {
        "code": "NL",
        "name": "Newfoundland and Labrador",
        "country_code": "CAN",
        "lat": 53.1355,
        "lng": -57.6604,
    },
    {
        "code": "NT-CA",
        "name": "Northwest Territories",
        "country_code": "CAN",
        "lat": 61.2181,
        "lng": -113.5170,
    },
    {"code": "NS", "name": "Nova Scotia", "country_code": "CAN", "lat": 44.6820, "lng": -63.7443},
    {"code": "NU", "name": "Nunavut", "country_code": "CAN", "lat": 70.2998, "lng": -83.1076},
    {"code": "ON", "name": "Ontario", "country_code": "CAN", "lat": 51.2538, "lng": -85.3232},
    {
        "code": "PE",
        "name": "Prince Edward Island",
        "country_code": "CAN",
        "lat": 46.5107,
        "lng": -63.4168,
    },
    {"code": "QC", "name": "Quebec", "country_code": "CAN", "lat": 53.1355, "lng": -73.2533},
    {"code": "SK", "name": "Saskatchewan", "country_code": "CAN", "lat": 52.9399, "lng": -106.4509},
    {"code": "YT", "name": "Yukon", "country_code": "CAN", "lat": 64.0685, "lng": -139.0623},
    # German States (Länder) - major ones
    {
        "code": "BW",
        "name": "Baden-Württemberg",
        "country_code": "DEU",
        "lat": 48.6616,
        "lng": 9.3501,
    },
    {"code": "BY", "name": "Bavaria", "country_code": "DEU", "lat": 49.0134, "lng": 11.4041},
    {"code": "BE", "name": "Berlin", "country_code": "DEU", "lat": 52.5200, "lng": 13.4050},
    {"code": "BB", "name": "Brandenburg", "country_code": "DEU", "lat": 52.4125, "lng": 12.5316},
    {"code": "HB", "name": "Bremen", "country_code": "DEU", "lat": 53.0793, "lng": 8.8017},
    {"code": "HH", "name": "Hamburg", "country_code": "DEU", "lat": 53.5511, "lng": 9.9937},
    {"code": "HE", "name": "Hesse", "country_code": "DEU", "lat": 50.6520, "lng": 9.1624},
    {
        "code": "MV",
        "name": "Mecklenburg-Western Pomerania",
        "country_code": "DEU",
        "lat": 53.6127,
        "lng": 12.4296,
    },
    {"code": "NI", "name": "Lower Saxony", "country_code": "DEU", "lat": 52.6367, "lng": 9.8451},
    {
        "code": "NW",
        "name": "North Rhine-Westphalia",
        "country_code": "DEU",
        "lat": 51.4332,
        "lng": 7.6616,
    },
    {
        "code": "RP",
        "name": "Rhineland-Palatinate",
        "country_code": "DEU",
        "lat": 49.9129,
        "lng": 7.4530,
    },
    {"code": "SL", "name": "Saarland", "country_code": "DEU", "lat": 49.3964, "lng": 6.6833},
    {"code": "SN", "name": "Saxony", "country_code": "DEU", "lat": 51.1045, "lng": 13.2017},
    {"code": "ST", "name": "Saxony-Anhalt", "country_code": "DEU", "lat": 51.9503, "lng": 11.6923},
    {
        "code": "SH",
        "name": "Schleswig-Holstein",
        "country_code": "DEU",
        "lat": 54.2194,
        "lng": 9.6961,
    },
    {"code": "TH", "name": "Thuringia", "country_code": "DEU", "lat": 50.9781, "lng": 11.0295},
    # UK Countries/Regions
    {"code": "ENG", "name": "England", "country_code": "GBR", "lat": 52.3555, "lng": -1.1743},
    {"code": "SCT", "name": "Scotland", "country_code": "GBR", "lat": 56.4907, "lng": -4.2026},
    {"code": "WLS", "name": "Wales", "country_code": "GBR", "lat": 52.1307, "lng": -3.7837},
    {
        "code": "NIR",
        "name": "Northern Ireland",
        "country_code": "GBR",
        "lat": 54.7877,
        "lng": -6.4923,
    },
    # Australian States
    {
        "code": "NSW",
        "name": "New South Wales",
        "country_code": "AUS",
        "lat": -31.2532,
        "lng": 146.9211,
    },
    {"code": "QLD", "name": "Queensland", "country_code": "AUS", "lat": -20.9176, "lng": 142.7028},
    {
        "code": "SA",
        "name": "South Australia",
        "country_code": "AUS",
        "lat": -29.9756,
        "lng": 134.4213,
    },
    {"code": "TAS", "name": "Tasmania", "country_code": "AUS", "lat": -41.4545, "lng": 145.9707},
    {"code": "VIC", "name": "Victoria", "country_code": "AUS", "lat": -36.5986, "lng": 144.6780},
    {
        "code": "WA-AU",
        "name": "Western Australia",
        "country_code": "AUS",
        "lat": -25.2764,
        "lng": 121.2914,
    },
    {
        "code": "ACT",
        "name": "Australian Capital Territory",
        "country_code": "AUS",
        "lat": -35.4735,
        "lng": 149.0124,
    },
    {
        "code": "NT-AU",
        "name": "Northern Territory",
        "country_code": "AUS",
        "lat": -19.4914,
        "lng": 132.5510,
    },
]


def seed_states(db: Session):
    """Seed states table with initial data"""
    print(f"  Checking for existing states...")

    # Check if states already exist
    existing_count = db.query(State).count()
    if existing_count > 0:
        print(f"  Found {existing_count} existing states, skipping...")
        return

    print(f"  Adding {len(STATES_DATA)} states...")

    # Get country mapping
    countries = db.query(Country).all()
    country_map = {country.code: country.id for country in countries}

    states_to_add = []
    skipped = 0

    for state_data in STATES_DATA:
        country_code = state_data.pop("country_code")

        if country_code not in country_map:
            print(
                f"  Warning: Country {country_code} not found, skipping state {state_data['name']}"
            )
            skipped += 1
            continue

        state_data["country_id"] = country_map[country_code]
        state = State(**state_data)
        states_to_add.append(state)

    db.add_all(states_to_add)
    db.commit()

    print(f"  Successfully added {len(states_to_add)} states")
    if skipped > 0:
        print(f"  Skipped {skipped} states due to missing countries")
