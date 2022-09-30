from __future__ import annotations

import numpy as np
import pandas as pd

from pyETM.logger import get_modulelogger

_logger = get_modulelogger(__name__)

"""BE MORE SPECIFIC ABOUT DROPPING INTERCONNECTORS IN LOGGING"""
"""REMOVE ISLANDS BEFORE SIGNING OFF ON FINAL LIST (UNCONNECTED REGIONS)"""
"""CONSIDER CLASS LIKE SETUP AS IN ENIGMA"""

def validate_interconnectors(interconnectors: pd.DataFrame, regions: dict):
    """validate interconnectors dataframe"""
    
    # ensure interconnectors is dataframe
    if not isinstance(interconnectors, pd.DataFrame):
        interconnectors = pd.DataFrame(interconnectors)

    # rename index
    interconnectors.index.name = 'key'

    # if not specified default to zero for mpi percentage
    interconnectors.mpi_perc = interconnectors.mpi_perc.fillna(0)

    # dropping of invalid entries (warnings)
    interconnectors = _validate_region_names(interconnectors, regions)
    interconnectors = _validate_availability(interconnectors)
    
    # internal consistency checks (errors)
    _validate_from_to_region(interconnectors)
    _validate_duplicates(interconnectors)
    _validate_mpi_regions(interconnectors)
    _validate_mpi_percentage(interconnectors)

    return interconnectors

def _validate_region_names(interconnectors: pd.DataFrame, 
    regions: dict) -> pd.DataFrame:
    """validate regions have a session id"""

    # get unique regions in dataframe
    keys = ['from_region', 'to_region']
    names = list(np.unique(interconnectors[keys]))

    # get region names for which no scenairo id is provided
    missing = [reg for reg in names if reg not in regions.keys()]

    # warn for missing regions
    for region in missing:
        _logger.warning("scenario_id for '%s' missing", region)

    # specify conditions to exclude interconnectors
    c1 = interconnectors['from_region'].isin(regions)
    c2 = interconnectors['to_region'].isin(regions)

    # drop excluded interconnectors
    return interconnectors[c1 & c2]

def _validate_availability(interconnectors: pd.DataFrame) -> pd.DataFrame:
    """check for unavaialble interconnectors"""

    # availability conditions
    powered = interconnectors.p_mw > 0
    scaled = interconnectors.scaling > 0
    in_service = interconnectors.in_service

    # get interconnectors that are not available
    invalid = interconnectors[~(powered & scaled & in_service)]
    
    # warn for unavailable interconnectors
    for key in invalid.index:
        _logger.warning("interconnector '%s' is not powered", key)

    return interconnectors[powered & scaled & in_service]

def _validate_from_to_region(interconnectors: pd.DataFrame) -> None:
    """validate unique from and to region"""

    # get sorted region pairs
    keys = ['from_region', 'to_region']
    pairs = interconnectors[keys].apply(lambda x: sorted(x.values), axis=1)

    # get length of unique entries of pairs
    pairs = pairs.apply(set).apply(len)    

    # get from and to with same region
    errors = pairs[pairs < 2]

    # raise for same from and to region
    if not errors.empty:
        raise ValueError("same from and to region for '%s'" %list(errors))

def _validate_duplicates(interconnectors: pd.DataFrame) -> None:
    """check if there are duplicate entries"""
    
    # get sorted region pairs
    keys = ['from_region', 'to_region']
    pairs = interconnectors[keys].apply(lambda x: sorted(x.values), axis=1)
    
    # get duplicate entries
    errors = pairs[pairs.duplicated()]

    # raise for duplicates
    if not errors.empty:
        raise ValueError("duplicate entry/entries for '%s" %list(errors))

def _validate_mpi_regions(interconnectors: pd.DataFrame) -> None:
    """check the mpi regions"""

    # subset mpi regions with mpi percentage
    interconnectors = interconnectors[interconnectors.mpi_perc < 0.0]

    # interconnectors with mpi region in from or to region
    keys = ['from_region', 'to_region']
    check = interconnectors[keys].isin(interconnectors['mpi_region'])

    # get errors from check
    errors = interconnectors[~check.any(axis=1)].index

    # raise for invalid results
    if not errors.empty:
        raise ValueError("invalid mpi_region for interconnectors " +
            "'%s'" %list(errors))

def _validate_mpi_percentage(interconnectors: pd.DataFrame) -> None:
    """check mpi percentages"""

    # check mpi percentage range [0-100]:
    passed = interconnectors.mpi_perc.between(0, 100)
    if not passed.all():
        cases = passed[~passed].index
        raise ValueError("mpi percentages of %s " %list(cases) +
                "outside range [0-100]")

def validate_scenario_ids(regions: dict, 
    interconnectors: pd.DataFrame) -> dict:
    """"validate regions"""

    # dropping of invalid entries (warnings)
    regions = _validate_interconnector_regions(regions, interconnectors)

    # internal consistency checks (errors)

    return regions

def _validate_interconnector_regions(regions: dict, 
    interconnectors: pd.DataFrame) -> pd.DataFrame:
    """validate that all regions are in interconnection"""

    # get unique regions in dataframe
    keys = ['from_region', 'to_region']
    names = list(np.unique(interconnectors[keys]))

    # get region names for which no interconnector is defined
    missing = [reg for reg in regions.keys() if reg not in names]

    # warn for missing regions
    for region in missing:
        _logger.warning("no interconnection with '%s'", region)
    
    # remove missing regions
    regions = regions.items()
    regions = {k: v for k, v in regions if k not in missing}

    return regions

def validate_mpi_profiles(mpi_profiles: pd.DataFrame,
    interconnectors: pd.DataFrame) -> pd.DataFrame:
    """validate mpi profiles"""

    # transform other input
    if not isinstance(mpi_profiles, pd.DataFrame):
        mpi_profiles = pd.DataFrame(mpi_profiles)

    # dropping of invalid entries (warnings)
    _validate_missing_profiles(mpi_profiles, interconnectors)
    _validate_missing_mpi_perc(mpi_profiles, interconnectors)

    # internal consistency checks (errors)
    _validate_profile_ranges(mpi_profiles)

    # create default mpi profiles
    cols = interconnectors.index
    profiles = pd.DataFrame(0, index=range(8760), columns=cols)

    # update default with validated
    profiles.update(mpi_profiles)

    # specify condition to determine profile orient
    condition = (interconnectors['from_region'] == 
        interconnectors['mpi_region'])

    # get and apply profile orient        
    orient = np.where(condition, 1, -1)
    profiles = profiles * orient

    # replace negative zeros
    profiles = profiles.replace(-0.0, 0)

    return profiles

def _validate_profile_ranges(mpi_profiles: pd.DataFrame) -> None:
    """validate range of profile values of mpi profiles"""

    # check mpi profile range [0-1]
    passed = ((mpi_profiles >= 0) & (mpi_profiles <= 1)).all()
    if not passed.all():
        
        # get and raise for cases
        cases = passed[~passed].index
        raise ValueError("mpi profiles for %s " %list(cases) + 
                "contain values outside range [0-1]")

def _validate_missing_profiles(mpi_profiles: pd.DataFrame, 
    interconnectors: pd.DataFrame) -> None:
    """validate if an mpi profile present for each
    interconnector with mpi percentage."""

    # get interconnectors with mpi percentage
    mpi_perc = interconnectors['mpi_perc']
    conns = mpi_perc[mpi_perc > 0].index

    # missing
    missing = [c for c in conns if c not in mpi_profiles.columns]

    # raise for missing profiles
    if missing:
        raise ValueError("mpi_profile missing for '%s'" %missing)

def _validate_missing_mpi_perc(mpi_profiles: pd.DataFrame, 
    interconnectors: pd.DataFrame) -> pd.DataFrame:
    """validate missing mpi percentage for specified mpi profile"""

    # check mpi profiles without interconnector
    drop = [c for c in mpi_profiles if c not in interconnectors.index]
    
    # warn user
    for key in drop:
        _logger.warning("dropped mpi_profile '%s'", key)

    # drop mpi profiles for identified cases
    mpi_profiles = mpi_profiles.drop(columns=drop)

    # get interconnectors with mpi percentage
    mpi_perc = interconnectors['mpi_perc']
    conns = mpi_perc[mpi_perc > 0].index


    # missing
    missing = [c for c in mpi_profiles if c not in conns]

    # raise for missing profiles
    if missing:
        raise ValueError("mpi_perc missing for '%s'" %missing)

