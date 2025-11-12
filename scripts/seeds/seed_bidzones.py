#!/usr/bin/env python3
"""
Seed script for bidzones table
"""

from sqlalchemy.orm import Session

from app.models.bidzone import Bidzone
from app.models.country import Country

BIDZONES_DATA = [
    {
        "code": "10Y1001A1001A39I",
        "name": "EE",
        "bidzone_type": "national",
        "country_codes": ["EST"],
        "lat": 58.5953,
        "lng": 25.0136,
    },
    {
        "code": "10Y1001A1001A44P",
        "name": "SE1",
        "bidzone_type": "regional",
        "country_codes": ["SWE"],
        "lat": 67.8558,
        "lng": 20.2253,
    },
    {
        "code": "10Y1001A1001A45N",
        "name": "SE2",
        "bidzone_type": "regional",
        "country_codes": ["SWE"],
        "lat": 62.3875,
        "lng": 16.3254,
    },
    {
        "code": "10Y1001A1001A46L",
        "name": "SE3",
        "bidzone_type": "regional",
        "country_codes": ["SWE"],
        "lat": 58.5877,
        "lng": 16.1925,
    },
    {
        "code": "10Y1001A1001A47J",
        "name": "SE4",
        "bidzone_type": "regional",
        "country_codes": ["SWE"],
        "lat": 55.6761,
        "lng": 12.5683,
    },
    {
        "code": "10Y1001A1001A48H",
        "name": "NO5",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 69.6492,
        "lng": 18.9553,
    },
    {
        "code": "10Y1001A1001A49F",
        "name": "RU",
        "bidzone_type": "regional",
        "country_codes": ["RUS"],
        "lat": 61.5240,
        "lng": 105.3188,
    },
    {
        "code": "10Y1001A1001A50U",
        "name": "RU-KGD",
        "bidzone_type": "regional",
        "country_codes": ["RUS"],
        "lat": 54.7104,
        "lng": 20.4522,
    },
    {
        "code": "10Y1001A1001A51S",
        "name": "BY",
        "bidzone_type": "national",
        "country_codes": ["BLR"],
        "lat": 53.7098,
        "lng": 27.9534,
    },
    {
        "code": "10Y1001A1001A59C",
        "name": "IE(SEM)",
        "bidzone_type": "national",
        "country_codes": ["IRL"],
        "lat": 53.1424,
        "lng": -7.6921,
    },
    {
        "code": "10Y1001A1001A63L",
        "name": "DE-AT-LU",
        "bidzone_type": "virtual",
        "country_codes": ["DEU", "AUT", "LUX"],
        "lat": 49.0,
        "lng": 10.0,
    },
    {
        "code": "10Y1001A1001A64J",
        "name": "NO1A",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 59.9139,
        "lng": 10.7522,
    },
    {
        "code": "10Y1001A1001A66F",
        "name": "IT-GR",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "GRC"],
        "lat": 40.0,
        "lng": 18.0,
    },
    {
        "code": "10Y1001A1001A67D",
        "name": "IT-North-SI",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "SVN"],
        "lat": 45.8,
        "lng": 13.6,
    },
    {
        "code": "10Y1001A1001A68B",
        "name": "IT-North-CH",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "CHE"],
        "lat": 46.0,
        "lng": 8.8,
    },
    {
        "code": "10Y1001A1001A699",
        "name": "IT-Brindisi",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 40.6384,
        "lng": 17.9463,
    },
    {
        "code": "10Y1001A1001A70O",
        "name": "IT-Centre-North",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 43.7711,
        "lng": 11.2486,
    },
    {
        "code": "10Y1001A1001A71M",
        "name": "IT-Centre-South",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 41.9,
        "lng": 13.5,
    },
    {
        "code": "10Y1001A1001A72K",
        "name": "IT-Foggia",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 41.4621,
        "lng": 15.5446,
    },
    {
        "code": "10Y1001A1001A73I",
        "name": "IT-North",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 45.4642,
        "lng": 9.1900,
    },
    {
        "code": "10Y1001A1001A74G",
        "name": "IT-Sardinia",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 40.1209,
        "lng": 9.0129,
    },
    {
        "code": "10Y1001A1001A75E",
        "name": "IT-Sicily",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 37.5999,
        "lng": 14.0153,
    },
    {
        "code": "10Y1001A1001A76C",
        "name": "IT-Priolo",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 37.1547,
        "lng": 15.1827,
    },
    {
        "code": "10Y1001A1001A77A",
        "name": "IT-Rossano",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 39.5769,
        "lng": 16.6319,
    },
    {
        "code": "10Y1001A1001A788",
        "name": "IT-South",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 40.5,
        "lng": 16.0,
    },
    {
        "code": "10Y1001A1001A80L",
        "name": "IT-North-AT",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "AUT"],
        "lat": 46.5,
        "lng": 11.5,
    },
    {
        "code": "10Y1001A1001A81J",
        "name": "IT-North-FR",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "FRA"],
        "lat": 44.5,
        "lng": 7.0,
    },
    {
        "code": "10Y1001A1001A82H",
        "name": "DE-LU",
        "bidzone_type": "virtual",
        "country_codes": ["DEU", "LUX"],
        "lat": 49.6116,
        "lng": 6.1319,
    },
    {
        "code": "10Y1001A1001A869",
        "name": "UA-DobTPP",
        "bidzone_type": "regional",
        "country_codes": ["UKR"],
        "lat": 48.5,
        "lng": 37.8,
    },
    {
        "code": "10Y1001A1001A877",
        "name": "IT-Malta",
        "bidzone_type": "virtual",
        "country_codes": ["ITA", "MLT"],
        "lat": 35.8997,
        "lng": 14.5146,
    },
    {
        "code": "10Y1001A1001A885",
        "name": "IT-SACOAC",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 40.0,
        "lng": 15.5,
    },
    {
        "code": "10Y1001A1001A893",
        "name": "IT-SACODC",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 40.0,
        "lng": 15.0,
    },
    {
        "code": "10Y1001A1001A93C",
        "name": "MT",
        "bidzone_type": "national",
        "country_codes": ["MLT"],
        "lat": 35.8997,
        "lng": 14.5146,
    },
    {
        "code": "10Y1001A1001A990",
        "name": "MD",
        "bidzone_type": "national",
        "country_codes": ["MDA"],
        "lat": 47.4116,
        "lng": 28.3699,
    },
    {
        "code": "10Y1001A1001B004",
        "name": "AM",
        "bidzone_type": "national",
        "country_codes": ["ARM"],
        "lat": 40.0691,
        "lng": 45.0382,
    },
    {
        "code": "10Y1001A1001B012",
        "name": "GE",
        "bidzone_type": "national",
        "country_codes": ["GEO"],
        "lat": 42.3154,
        "lng": 43.3569,
    },
    {
        "code": "10Y1001A1001B05V",
        "name": "AZ",
        "bidzone_type": "national",
        "country_codes": ["AZE"],
        "lat": 40.1431,
        "lng": 47.5769,
    },
    {
        "code": "10Y1001C--00003F",
        "name": "UA",
        "bidzone_type": "national",
        "country_codes": ["UKR"],
        "lat": 48.3794,
        "lng": 31.1656,
    },
    {
        "code": "10Y1001C--000182",
        "name": "UA-IPS",
        "bidzone_type": "regional",
        "country_codes": ["UKR"],
        "lat": 48.5,
        "lng": 32.0,
    },
    {
        "code": "10Y1001C--00096J",
        "name": "IT-Calabria",
        "bidzone_type": "regional",
        "country_codes": ["ITA"],
        "lat": 39.3098,
        "lng": 16.2476,
    },
    {
        "code": "10Y1001C--00098F",
        "name": "GB(IFA)",
        "bidzone_type": "interconnector",
        "country_codes": ["GBR"],
        "lat": 51.5074,
        "lng": -0.1278,
    },
    {
        "code": "10Y1001C--00100H",
        "name": "XK",
        "bidzone_type": "national",
        "country_codes": ["XKX"],
        "lat": 42.6026,
        "lng": 20.9030,
    },
    {
        "code": "10Y1001C--001219",
        "name": "NO2A",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 60.1282,
        "lng": 18.6435,
    },
    {
        "code": "10YAL-KESH-----5",
        "name": "AL",
        "bidzone_type": "national",
        "country_codes": ["ALB"],
        "lat": 41.1533,
        "lng": 20.1683,
    },
    {
        "code": "10YAT-APG------L",
        "name": "AT",
        "bidzone_type": "national",
        "country_codes": ["AUT"],
        "lat": 47.5162,
        "lng": 14.5501,
    },
    {
        "code": "10YBA-JPCC-----D",
        "name": "BA",
        "bidzone_type": "national",
        "country_codes": ["BIH"],
        "lat": 43.9159,
        "lng": 17.6791,
    },
    {
        "code": "10YBE----------2",
        "name": "BE",
        "bidzone_type": "national",
        "country_codes": ["BEL"],
        "lat": 50.5039,
        "lng": 4.4699,
    },
    {
        "code": "10YCA-BULGARIA-R",
        "name": "BG",
        "bidzone_type": "national",
        "country_codes": ["BGR"],
        "lat": 42.7339,
        "lng": 25.4858,
    },
    {
        "code": "10YCS-CG-TSO---S",
        "name": "ME",
        "bidzone_type": "national",
        "country_codes": ["MNE"],
        "lat": 42.7087,
        "lng": 19.3744,
    },
    {
        "code": "10YCS-SERBIATSOV",
        "name": "RS",
        "bidzone_type": "national",
        "country_codes": ["SRB"],
        "lat": 44.0165,
        "lng": 21.0059,
    },
    {
        "code": "10YCY-1001A0003J",
        "name": "CY",
        "bidzone_type": "national",
        "country_codes": ["CYP"],
        "lat": 35.1264,
        "lng": 33.4299,
    },
    {
        "code": "10YCZ-CEPS-----N",
        "name": "CZ",
        "bidzone_type": "national",
        "country_codes": ["CZE"],
        "lat": 49.8175,
        "lng": 15.4730,
    },
    {
        "code": "10YDK-1-------AA",
        "name": "DK1A",
        "bidzone_type": "regional",
        "country_codes": ["DNK"],
        "lat": 56.2639,
        "lng": 9.5018,
    },
    {
        "code": "10YDK-1--------W",
        "name": "DK1",
        "bidzone_type": "regional",
        "country_codes": ["DNK"],
        "lat": 56.2639,
        "lng": 9.5018,
    },
    {
        "code": "10YDK-2--------M",
        "name": "DK2",
        "bidzone_type": "regional",
        "country_codes": ["DNK"],
        "lat": 55.6761,
        "lng": 12.5683,
    },
    {
        "code": "10YDOM-CZ-DE-SKK",
        "name": "CZ+DE+SK",
        "bidzone_type": "virtual",
        "country_codes": ["CZE", "DEU", "SVK"],
        "lat": 50.0,
        "lng": 14.0,
    },
    {
        "code": "10YES-REE------0",
        "name": "ES",
        "bidzone_type": "national",
        "country_codes": ["ESP"],
        "lat": 40.4637,
        "lng": -3.7492,
    },
    {
        "code": "10YFI-1--------U",
        "name": "FI",
        "bidzone_type": "national",
        "country_codes": ["FIN"],
        "lat": 61.9241,
        "lng": 25.7482,
    },
    {
        "code": "10YFR-RTE------C",
        "name": "FR",
        "bidzone_type": "national",
        "country_codes": ["FRA"],
        "lat": 46.2276,
        "lng": 2.2137,
    },
    {
        "code": "10YGR-HTSO-----Y",
        "name": "GR",
        "bidzone_type": "national",
        "country_codes": ["GRC"],
        "lat": 39.0742,
        "lng": 21.8243,
    },
    {
        "code": "10YHR-HEP------M",
        "name": "HR",
        "bidzone_type": "national",
        "country_codes": ["HRV"],
        "lat": 45.1000,
        "lng": 15.2000,
    },
    {
        "code": "10YHU-MAVIR----U",
        "name": "HU",
        "bidzone_type": "national",
        "country_codes": ["HUN"],
        "lat": 47.1625,
        "lng": 19.5033,
    },
    {
        "code": "10YLT-1001A0008Q",
        "name": "LT",
        "bidzone_type": "national",
        "country_codes": ["LTU"],
        "lat": 55.1694,
        "lng": 23.8813,
    },
    {
        "code": "10YLV-1001A00074",
        "name": "LV",
        "bidzone_type": "national",
        "country_codes": ["LVA"],
        "lat": 56.8796,
        "lng": 24.6032,
    },
    {
        "code": "10YMK-MEPSO----8",
        "name": "MK",
        "bidzone_type": "national",
        "country_codes": ["MKD"],
        "lat": 41.6086,
        "lng": 21.7453,
    },
    {
        "code": "10YNL----------L",
        "name": "NL",
        "bidzone_type": "national",
        "country_codes": ["NLD"],
        "lat": 52.1326,
        "lng": 5.2913,
    },
    {
        "code": "10YNO-1--------2",
        "name": "NO1",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 59.9139,
        "lng": 10.7522,
    },
    {
        "code": "10YNO-2--------T",
        "name": "NO2",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 60.4720,
        "lng": 8.4689,
    },
    {
        "code": "10YNO-3--------J",
        "name": "NO3",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 63.4305,
        "lng": 10.3951,
    },
    {
        "code": "10YNO-4--------9",
        "name": "NO4",
        "bidzone_type": "regional",
        "country_codes": ["NOR"],
        "lat": 69.6492,
        "lng": 18.9553,
    },
    {
        "code": "10YPL-AREA-----S",
        "name": "PL",
        "bidzone_type": "national",
        "country_codes": ["POL"],
        "lat": 51.9194,
        "lng": 19.1451,
    },
    {
        "code": "10YRO-TEL------P",
        "name": "RO",
        "bidzone_type": "national",
        "country_codes": ["ROU"],
        "lat": 45.9432,
        "lng": 24.9668,
    },
    {
        "code": "10YSI-ELES-----O",
        "name": "SI",
        "bidzone_type": "national",
        "country_codes": ["SVN"],
        "lat": 46.1512,
        "lng": 14.9955,
    },
    {
        "code": "10YSK-SEPS-----K",
        "name": "SK",
        "bidzone_type": "national",
        "country_codes": ["SVK"],
        "lat": 48.6690,
        "lng": 19.6990,
    },
    {
        "code": "10YTR-TEIAS----W",
        "name": "TR",
        "bidzone_type": "national",
        "country_codes": ["TUR"],
        "lat": 38.9637,
        "lng": 35.2433,
    },
    {
        "code": "10YUA-WEPS-----0",
        "name": "UA-BEI",
        "bidzone_type": "regional",
        "country_codes": ["UKR"],
        "lat": 47.8388,
        "lng": 35.1396,
    },
    {
        "code": "11Y0-0000-0265-K",
        "name": "GB(ElecLink)",
        "bidzone_type": "interconnector",
        "country_codes": ["GBR"],
        "lat": 51.5074,
        "lng": -0.1278,
    },
    {
        "code": "17Y0000009369493",
        "name": "GB(IFA2)",
        "bidzone_type": "interconnector",
        "country_codes": ["GBR"],
        "lat": 51.5074,
        "lng": -0.1278,
    },
    {
        "code": "46Y000000000007M",
        "name": "DK1-NO1",
        "bidzone_type": "interconnector",
        "country_codes": ["DNK", "NOR"],
        "lat": 57.0,
        "lng": 10.0,
    },
    {
        "code": "50Y0JVU59B4JWQCU",
        "name": "NO2NSL",
        "bidzone_type": "interconnector",
        "country_codes": ["NOR"],
        "lat": 60.4720,
        "lng": 8.4689,
    },
    {
        "code": "10YGB----------A",
        "name": "GB",
        "bidzone_type": "national",
        "country_codes": ["GBR"],
        "lat": 55.3781,
        "lng": -3.4360,
    },
]


def seed_bidzones(db: Session):
    """Seed bidzones table with initial data"""
    print(f"  Checking for existing bidzones...")

    # Get existing bidzone codes
    existing_codes = {b.code for b in db.query(Bidzone.code).all()}

    # Get all countries for mapping
    countries = db.query(Country).all()
    country_map = {c.code: c for c in countries}

    # Filter out bidzones that already exist
    bidzones_to_add = []
    skipped_count = 0

    for bidzone_data in BIDZONES_DATA:
        if bidzone_data["code"] in existing_codes:
            skipped_count += 1
            continue

        # Extract country codes (without modifying original data)
        country_codes = bidzone_data.get("country_codes", [])

        # Create bidzone
        bidzone = Bidzone(
            code=bidzone_data["code"],
            name=bidzone_data["name"],
            bidzone_type=bidzone_data["bidzone_type"],
            lat=bidzone_data.get("lat"),
            lng=bidzone_data.get("lng"),
            polygon_wkt=bidzone_data.get("polygon_wkt"),
        )

        # Add country relationships
        countries_to_add = []
        for country_code in country_codes:
            if country_code in country_map:
                countries_to_add.append(country_map[country_code])
            else:
                print(
                    f"  Warning: Country code {country_code} not found for bidzone {bidzone_data['name']}"
                )

        bidzone.countries = countries_to_add
        bidzones_to_add.append(bidzone)

    if not bidzones_to_add:
        print(f"  Found {len(existing_codes)} existing bidzones, no new bidzones to add")
        if skipped_count > 0:
            print(f"  Skipped {skipped_count} existing bidzones")
        return

    print(f"  Adding {len(bidzones_to_add)} new bidzones...")
    if skipped_count > 0:
        print(f"  Skipping {skipped_count} existing bidzones")

    db.add_all(bidzones_to_add)
    db.commit()

    print(f"  Successfully added {len(bidzones_to_add)} bidzones")
