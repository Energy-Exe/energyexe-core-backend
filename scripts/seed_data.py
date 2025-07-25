#!/usr/bin/env python3
"""
Main seed data script for EnergyExe
Coordinates seeding of all data tables in the correct order.
"""

import sys
from pathlib import Path
from typing import Dict

# Add parent directory to path to import app modules
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.models.bidzone import Bidzone
from app.models.cable import Cable
from app.models.control_area import ControlArea

# Import all models to ensure relationships are properly configured
from app.models.country import Country
from app.models.market_balance_area import MarketBalanceArea
from app.models.owner import Owner
from app.models.project import Project
from app.models.region import Region
from app.models.state import State
from app.models.substation import Substation
from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit
from app.models.user import User
from app.models.windfarm import Windfarm
from app.models.windfarm_owner import WindfarmOwner
from scripts.seeds.seed_bidzones import seed_bidzones
from scripts.seeds.seed_control_areas import seed_control_areas
from scripts.seeds.seed_countries import seed_countries
from scripts.seeds.seed_market_balance_areas import seed_market_balance_areas
from scripts.seeds.seed_owners import seed_owners
from scripts.seeds.seed_regions import seed_regions
from scripts.seeds.seed_states import seed_states
from scripts.seeds.seed_turbine_models import seed_turbine_models
from scripts.seeds.seed_windfarms import seed_windfarms


def get_sync_db_session():
    """Get synchronous database session"""
    settings = get_settings()
    sync_engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
    return SessionLocal()


def run_seeds():
    """Run all seed scripts in the correct order"""
    print("🌱 Starting database seeding process...")

    # Use sync session for seeding
    db = get_sync_db_session()

    try:
        # Seed countries first (no dependencies)
        print("\n📍 Seeding countries...")
        seed_countries(db)
        print("✅ Countries seeded successfully")

        # Seed states (depends on countries)
        print("\n🏛️ Seeding states...")
        seed_states(db)
        print("✅ States seeded successfully")

        # Seed regions (no dependencies)
        print("\n🌊 Seeding regions...")
        seed_regions(db)
        print("✅ Regions seeded successfully")

        # Seed bidzones (depends on countries)
        print("\n⚡ Seeding bidzones...")
        seed_bidzones(db)
        print("✅ Bidzones seeded successfully")

        # Seed control areas (depends on countries)
        print("\n🎛️ Seeding control areas...")
        seed_control_areas(db)
        print("✅ Control areas seeded successfully")

        # Seed market balance areas (depends on countries)
        print("\n⚖️ Seeding market balance areas...")
        seed_market_balance_areas(db)
        print("✅ Market balance areas seeded successfully")

        # Seed owners (no dependencies)
        print("\n🏢 Seeding owners...")
        seed_owners(db)
        print("✅ Owners seeded successfully")

        # Seed turbine models (no dependencies)
        print("\n⚙️ Seeding turbine models...")
        seed_turbine_models(db)
        print("✅ Turbine models seeded successfully")

        # Seed windfarms (depends on countries, states, regions, bidzones, market_balance_areas, control_areas, owners)
        print("\n🌬️ Seeding windfarms...")
        seed_windfarms(db)
        print("✅ Windfarms seeded successfully")

        print("\n🎉 Database seeding completed successfully!")

    except Exception as e:
        print(f"❌ Error during seeding: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def check_existing_data(db: Session) -> Dict[str, int]:
    """Check existing data counts"""
    counts = {
        "countries": db.query(Country).count(),
        "states": db.query(State).count(),
        "regions": db.query(Region).count(),
        "bidzones": db.query(Bidzone).count(),
        "control_areas": db.query(ControlArea).count(),
        "market_balance_areas": db.query(MarketBalanceArea).count(),
        "owners": db.query(Owner).count(),
        "turbine_models": db.query(TurbineModel).count(),
        "windfarms": db.query(Windfarm).count(),
    }
    return counts


def main():
    """Main function"""
    print("🔍 Checking current database state...")

    db = get_sync_db_session()
    try:
        counts = check_existing_data(db)
        print(f"Current data counts:")
        for table, count in counts.items():
            print(f"  {table}: {count}")

        if "-y" not in sys.argv and any(count > 0 for count in counts.values()):
            response = input("\n⚠️  Database already contains data. Continue? (y/N): ")
            if response.lower() != "y":
                print("Seeding cancelled.")
                return
    finally:
        db.close()

    # Run the seeding process
    run_seeds()


if __name__ == "__main__":
    main()
