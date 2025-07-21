#!/usr/bin/env python3
"""
Seed script for regions table
"""

from sqlalchemy.orm import Session

from app.models.region import Region

REGIONS_DATA = [
    {
        "code": "SNS",
        "name": "Scottish North Sea",
        "location_type": "sea",
        "lat": 57.5,
        "lng": 1.0,
    },
    {
        "code": "GEB",
        "name": "German Bight",
        "location_type": "sea",
        "lat": 54.0,
        "lng": 7.5,
    },
    {
        "code": "KAT",
        "name": "Kattegatt",
        "location_type": "sea",
        "lat": 57.0,
        "lng": 11.5,
    },
    {
        "code": "BAL",
        "name": "Baltic Sea",
        "location_type": "sea",
        "lat": 59.0,
        "lng": 19.0,
    },
    {
        "code": "IRS",
        "name": "Irish Sea",
        "location_type": "sea",
        "lat": 54.0,
        "lng": -4.5,
    },
    {
        "code": "SNS_S",
        "name": "Southern North Sea",
        "location_type": "sea",
        "lat": 52.0,
        "lng": 3.0,
    },
    {
        "code": "USA_ATL",
        "name": "US Atlantic",
        "location_type": "sea",
        "lat": 38.0,
        "lng": -73.0,
    },
    {
        "code": "TWS",
        "name": "Taiwan Strait",
        "location_type": "sea",
        "lat": 23.5,
        "lng": 119.0,
    },
    {
        "code": "ENC",
        "name": "English Channel",
        "location_type": "sea",
        "lat": 50.0,
        "lng": 0.0,
    },
    {
        "code": "BOB",
        "name": "Bay of Biscay",
        "location_type": "sea",
        "lat": 45.0,
        "lng": -4.0,
    },
    {
        "code": "EUR_ATL",
        "name": "European Atlantic",
        "location_type": "sea",
        "lat": 47.0,
        "lng": -8.0,
    },
    {
        "code": "CENS",
        "name": "Central East North Sea",
        "location_type": "sea",
        "lat": 56.0,
        "lng": 2.5,
    },
    {
        "code": "WSC",
        "name": "West Scotland",
        "location_type": "sea",
        "lat": 56.5,
        "lng": -6.0,
    },
    {
        "code": "SKA",
        "name": "Skagerrak",
        "location_type": "sea",
        "lat": 58.0,
        "lng": 9.0,
    },
]


def seed_regions(db: Session):
    """Seed regions table with initial data"""
    print(f"  Checking for existing regions...")

    # Check if regions already exist
    existing_count = db.query(Region).count()
    if existing_count > 0:
        print(f"  Found {existing_count} existing regions, skipping...")
        return

    print(f"  Adding {len(REGIONS_DATA)} regions...")

    regions_to_add = []
    for region_data in REGIONS_DATA:
        region = Region(**region_data)
        regions_to_add.append(region)

    db.add_all(regions_to_add)
    db.commit()

    print(f"  Successfully added {len(regions_to_add)} regions")
