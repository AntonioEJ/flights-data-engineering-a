"""
Flights Database Setup — SQLAlchemy 2.0
Siguiendo el patrón del demo northwind_etl.ipynb del curso.

Uso:
    python db/setup_db.py \
        --endpoint <RdsEndpoint> \
        --password <DBPassword> \
        --data-dir data/
"""

import argparse
import sys
import pandas as pd
from typing import Optional

from sqlalchemy import create_engine, String, Integer, SmallInteger, Float, ForeignKey, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


# ---------------------------------------------------------------------------
# 1. Modelos SQLAlchemy 2.0 — misma convención que Northwind del curso
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code:  Mapped[str] = mapped_column(String(2), primary_key=True)
    airline:    Mapped[str] = mapped_column(String(100))


class Airport(Base):
    __tablename__ = "airports"

    iata_code:  Mapped[str]            = mapped_column(String(5), primary_key=True)
    airport:    Mapped[str]            = mapped_column(String(200))
    city:       Mapped[Optional[str]]  = mapped_column(String(100))
    state:      Mapped[Optional[str]]  = mapped_column(String(2))
    country:    Mapped[Optional[str]]  = mapped_column(String(50))
    latitude:   Mapped[Optional[float]] = mapped_column(Float)
    longitude:  Mapped[Optional[float]] = mapped_column(Float)


class Flight(Base):
    __tablename__ = "flights"

    id:                     Mapped[int]            = mapped_column(Integer, primary_key=True, autoincrement=True)
    year:                   Mapped[Optional[int]]  = mapped_column(SmallInteger)
    month:                  Mapped[Optional[int]]  = mapped_column(SmallInteger)
    day:                    Mapped[Optional[int]]  = mapped_column(SmallInteger)
    day_of_week:            Mapped[Optional[int]]  = mapped_column(SmallInteger)
    airline:                Mapped[Optional[str]]  = mapped_column(String(2), ForeignKey("airlines.iata_code"))
    flight_number:          Mapped[Optional[int]]  = mapped_column(Integer)
    tail_number:            Mapped[Optional[str]]  = mapped_column(String(10))
    origin_airport:         Mapped[Optional[str]]  = mapped_column(String(5), ForeignKey("airports.iata_code"))
    destination_airport:    Mapped[Optional[str]]  = mapped_column(String(5), ForeignKey("airports.iata_code"))
    scheduled_departure:    Mapped[Optional[int]]  = mapped_column(Integer)
    departure_time:         Mapped[Optional[float]] = mapped_column(Float)
    departure_delay:        Mapped[Optional[float]] = mapped_column(Float)
    taxi_out:               Mapped[Optional[float]] = mapped_column(Float)
    wheels_off:             Mapped[Optional[float]] = mapped_column(Float)
    scheduled_time:         Mapped[Optional[float]] = mapped_column(Float)
    elapsed_time:           Mapped[Optional[float]] = mapped_column(Float)
    air_time:               Mapped[Optional[float]] = mapped_column(Float)
    distance:               Mapped[Optional[float]] = mapped_column(Float)
    wheels_on:              Mapped[Optional[float]] = mapped_column(Float)
    taxi_in:                Mapped[Optional[float]] = mapped_column(Float)
    scheduled_arrival:      Mapped[Optional[int]]  = mapped_column(Integer)
    arrival_time:           Mapped[Optional[float]] = mapped_column(Float)
    arrival_delay:          Mapped[Optional[float]] = mapped_column(Float)
    diverted:               Mapped[Optional[int]]  = mapped_column(SmallInteger)
    cancelled:              Mapped[Optional[int]]  = mapped_column(SmallInteger)
    cancellation_reason:    Mapped[Optional[str]]  = mapped_column(String(1))
    air_system_delay:       Mapped[Optional[float]] = mapped_column(Float)
    security_delay:         Mapped[Optional[float]] = mapped_column(Float)
    airline_delay:          Mapped[Optional[float]] = mapped_column(Float)
    late_aircraft_delay:    Mapped[Optional[float]] = mapped_column(Float)
    weather_delay:          Mapped[Optional[float]] = mapped_column(Float)


# ---------------------------------------------------------------------------
# 2. Función de carga — mismo patrón que load_csv del demo Northwind
# ---------------------------------------------------------------------------
def load_csv(session, model, path, nrows=None):
    """Carga un CSV a PostgreSQL usando bulk insert de SQLAlchemy 2.0."""
    df = pd.read_csv(path, nrows=nrows)

    # Normalizar nombres de columnas a lowercase (CSV usa UPPERCASE)
    df.columns = df.columns.str.lower()

    # pd.isnull() maneja tanto NaN (float) como NaT (datetime) → None para PostgreSQL
    records = [
        {k: None if pd.isnull(v) else v for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]

    session.execute(insert(model), records)
    print(f"✓ {model.__tablename__}: {len(records):,} filas cargadas")


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Setup flights database in RDS")
    parser.add_argument("--endpoint", required=True, help="RDS primary endpoint")
    parser.add_argument("--user", default="itam", help="DB username")
    parser.add_argument("--password", required=True, help="DB password")
    parser.add_argument("--dbname", default="flights", help="Database name")
    parser.add_argument("--port", default=5432, type=int, help="DB port")
    parser.add_argument("--data-dir", required=True, help="Path to data/ directory with CSVs")
    args = parser.parse_args()

    # --- Conexión al endpoint primario (escrituras) ---
    engine = create_engine(
        f"postgresql+psycopg2://{args.user}:{args.password}@{args.endpoint}:{args.port}/{args.dbname}"
    )

    # Verificar conexión
    try:
        with engine.connect() as conn:
            print("✓ Conexión exitosa a RDS")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        sys.exit(1)

    # --- Idempotencia: drop en orden inverso de FK, luego create ---
    print("Creando tablas...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("✓ Tablas creadas: airlines, airports, flights")

    # --- Carga de datos ---
    # Orden respeta dependencias FK:
    #   sin FK: airlines, airports
    #   depende de ambas: flights
    data_dir = args.data_dir.rstrip("/")

    with Session(engine) as session:
        load_csv(session, Airline, f"{data_dir}/airlines.csv")
        load_csv(session, Airport, f"{data_dir}/airports.csv")
        load_csv(session, Flight,  f"{data_dir}/flights.csv", nrows=500_000)
        session.commit()
        print("✓ Bootstrap completo")


if __name__ == "__main__":
    main()