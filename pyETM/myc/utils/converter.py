from __future__ import annotations

import pandas as pd

from typing import TYPE_CHECKING

from pyETM import Client
from pyETM.myc import Model
from pyETM.optional import import_optional_dependency

def copy_study_session_ids(session_ids: pd.Series | Model, 
    study: str | None = None, metadata: dict | None = None, 
    keep_compatible: bool = False, **kwargs) -> pd.Series:
    """make a copy of an existing study. The returned
    session ids are decoupled from the original study,
    but contain the same values"""

    # load study session ids from model
    if isinstance(session_ids, Model):
        kwargs = {**session_ids._kwargs, **kwargs}
        session_ids = session_ids.session_ids.copy()

    # make series-like object
    if not isinstance(session_ids, pd.Series):
        session_ids = pd.Series(session_ids, name='SESSION')

    # helper function
    def scenario_copy(session_id):
        """create compatible scenario copy"""

        # initiate client from existing scenairo
        client = Client.from_existing_scenario(session_id, metadata=metadata, 
            keep_compatible=keep_compatible, **kwargs)

        return client.scenario_id

    # make copies of session ids
    session_ids = session_ids.apply(scenario_copy)

    # set study if applicable
    if study is not None:
        session_ids = session_ids.index.set_levels([study], level='STUDY')

    return session_ids

def copy_study_configuration(filepath: str, model: Model, 
    study: str | None = None, copy_session_ids: bool = True,
    metadata: dict | None = None, keep_compatible: bool = False) -> None:
    """copy study configuration"""

    from pathlib import Path
    from pyETM.myc.utils import add_series, add_frame

    if TYPE_CHECKING:
        # import xlsxwriter
        import xlsxwriter
        
    else:
        # import optional dependency
        xlsxwriter = import_optional_dependency('xlsxwriter')
    
    # check filepath
    if not Path(filepath).parent.exists:
        raise FileNotFoundError("Path to file does not exist: '%s'"
            %filepath)

    # create workbook
    workbook = xlsxwriter.Workbook(str(filepath))

    # get session ids
    if copy_session_ids:
        
        # create copies of session ids
        sessions = copy_study_session_ids(model, study=study,
            metadata=metadata, keep_compatible=keep_compatible)

    else:

        # keep original
        sessions = model.session_ids

    # add sessions and set column width
    add_series('Sessions', sessions, workbook, column_width=18)

    # add parameters and set column width
    add_series('Parameters', model.parameters, workbook, 
        index_width=80, column_width=18)

    # add gqueries and set column width
    add_series('GQueries', model.gqueries, workbook,
        index_width=80, column_width=18)

    # add mapping and set column width
    if model.mapping is not None:
        add_frame('Mapping', model.mapping, workbook,
            index_width=[80, 18], column_width=18)

    # write workbook
    workbook.close()
