"""lookup method"""
import numpy as np
import pandas as pd


def lookup_coordinates(coords: pd.Series, frame: pd.DataFrame, **kwargs) -> pd.Series:
    """lookup function to get coordinate values from dataframe"""

    # reindex frame with factorized coords
    idx, cols = pd.factorize(coords)
    values = frame.reindex(cols, axis=1)

    # lookup values
    values = values.to_numpy()
    values = values[np.arange(len(values)), idx]

    return pd.Series(values, index=frame.index, **kwargs)
