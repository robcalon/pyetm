from __future__ import annotations

import pandas as pd

from typing import TYPE_CHECKING

from pyETM import Client
from pyETM.myc import Model
from pyETM.optional import import_optional_dependency

def copy_study_session_ids(model: Model, study: str | None = None,
    metadata: dict | None = None, keep_compatible: bool = False) -> pd.Series:
    """make a copy of an existing study. The returned
    session ids are decoupled from the original study,
    but contain the same values"""

    # load study session ids
    session_ids = model.session_ids.copy()

    def scenario_copy(session_id):
        """create compatible scenario copy"""

        # initiate client from existing scenairo
        client = Client.from_existing_scenario(session_id, metadata=metadata, 
            keep_compatible=keep_compatible, **model._kwargs)

        return client.scenario_id

    # make copies of session ids
    session_ids = session_ids.apply(scenario_copy)

    # set study if applicable
    if study is not None:
        session_ids = session_ids.index.set_levels([study], level='STUDY')

    return session_ids

def copy_study_configuration(filepath: str, model: Model, 
    study: str | None = None, metadata: dict | None = None, 
    keep_compatible: bool = False) -> None:
    """copy study configuration"""

    from pathlib import Path
    from pyETM.myc.utils import add_series

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

    # copy session ids
    sessions = copy_study_session_ids(model, study=study,
        metadata=metadata, keep_compatible=keep_compatible)

    # add sessions and set column width
    worksheet = add_series('Sessions', sessions, workbook)
    worksheet.set_column(0, 4, 20)

    # add parameters and set column width
    worksheet = add_series('Parameters', model.parameters, workbook)
    worksheet.set_column(0, 0, 85)
    worksheet.set_column(1, 1, 20)

    # add gqueries and set column width
    worksheet = add_series('GQueries', model.gqueries, workbook)
    worksheet.set_column(0, 0, 85)
    worksheet.set_column(1, 1, 20)

    # write workbook
    workbook.close()
