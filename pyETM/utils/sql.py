"""Helper to write DataFrames to sql database"""
from __future__ import annotations

from typing import TYPE_CHECKING
from pyETM.optional import import_optional_dependency

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, URL

def frame_to_sql(
    frame: pd.DataFrame,
    engine: Engine | URL | str,
    table: str,
    index: int,
    index_name : str,
    **kwargs
) -> None:
    """"Add frame to an sql database with an
    sqlalchemy engine, e.g. a sqlite database.

    Specifically designed to easily store weather
    profiles for multiple climate years or curves
    for different scenarios."""

    if TYPE_CHECKING:
        import sqlalchemy

    else:
        # import optional dependency
        sqlalchemy = import_optional_dependency('sqlalchemy')

    if not isinstance(engine, sqlalchemy.engine.Engine):
        engine = sqlalchemy.create_engine(engine, **kwargs)

    # reflect engine
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)

    if table in metadata.tables.keys():

        # get passed table
        mtable = metadata.tables[table]

        # get column sets
        cols = set(frame.columns)
        keys = set(mtable.columns.keys()[2:])

        # raise on conflict
        if not cols == keys:
            raise ValueError(
                "Profiles and table are misalligned for columns: "
                f"{keys.symmetric_difference(cols)}")

        # get present index values
        query = f"SELECT DISTINCT {index_name} FROM {table}"
        present = pd.read_sql(query, con=engine).squeeze(axis=1)

        # raise on conflict
        if index in present:
            raise ValueError(
                f"Values for index '{index}' already present "
                f"in table for zone '{table}'")

    # reset existing index
    frame = frame.reset_index(drop=True)

    # add index name to frame
    names = [index_name, 'hour']
    frame = pd.concat([frame], keys=[index], names=names)

    # add frame to engine
    return frame.to_sql(name=table, con=engine, if_exists='append')
