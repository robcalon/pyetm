"""conversion methods"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from pyetm import Client
from pyetm.logger import get_modulelogger
from pyetm.myc import MYCClient
from pyetm.optional import import_optional_dependency
from pyetm.utils import add_frame, add_series

_logger = get_modulelogger(__name__)

if TYPE_CHECKING:
    pass


def copy_study_session_ids(
    session_ids: pd.Series | MYCClient,
    study: str | None = None,
    metadata: dict | None = None,
    keep_compatible: bool = False,
    **kwargs,
) -> pd.Series:
    """make a copy of an existing study. The returned
    session ids are decoupled from the original study,
    but contain the same values"""

    # load study session ids from model
    if isinstance(session_ids, MYCClient):
        kwargs = {**session_ids._kwargs, **kwargs}
        session_ids = session_ids.session_ids.copy()

    # make series-like object
    if not isinstance(session_ids, pd.Series):
        session_ids = pd.Series(session_ids, name="SESSION")

    # helper function
    def scenario_copy(session_id):
        """create compatible scenario copy"""

        # initiate client from existing scenairo
        client = Client.from_existing_scenario(
            session_id, metadata=metadata, keep_compatible=keep_compatible, **kwargs
        )

        return client.scenario_id

    # make copies of session ids
    session_ids = session_ids.apply(scenario_copy)

    # set study if applicable
    if study is not None:
        session_ids = session_ids.index.set_levels([study], level="STUDY")

    return session_ids


def copy_study_configuration(
    filepath: str,
    model: MYCClient,
    study: str | None = None,
    copy_session_ids: bool = True,
    metadata: dict | None = None,
    keep_compatible: bool = False,
) -> None:
    """copy study configuration"""

    # import optional dependency
    xlsxwriter = import_optional_dependency("xlsxwriter")

    # check filepath
    if not Path(filepath).parent.exists:
        raise FileNotFoundError(f"Path to file does not exist: '{filepath}'")

    # create workbook
    workbook = xlsxwriter.Workbook(str(filepath))

    # get session ids
    if copy_session_ids:
        # create copies of session ids
        sessions = copy_study_session_ids(
            model, study=study, metadata=metadata, keep_compatible=keep_compatible
        )

    else:
        _logger.warning(
            "when using 'copy_session_ids=False', the "
            + "'keep_compatible' argument is ignored."
        )

        # keep original
        sessions = model.session_ids

        # set study if applicable
        if study is not None:
            sessions = sessions.index.set_levels([study], level="STUDY")

            _logger.warning(
                "'study' passed without copying session_ids, "
                + "it is recommended to use 'copy_session_ids=True' instead to "
                + "prevent referencing the same session_id by different names."
            )

        if metadata is not None:
            _logger.warning(
                "'metadata' passed without copying session_ids, "
                + "use 'copy_session_ids=True' instead."
            )

    # add sessions and set column width
    add_series("Sessions", sessions, workbook, column_width=18)

    # add parameters and set column width
    add_series(
        "Parameters", model.parameters, workbook, index_width=80, column_width=18
    )

    # add gqueries and set column width
    add_series("GQueries", model.gqueries, workbook, index_width=80, column_width=18)

    # add mapping and set column width
    if model.mapping is not None:
        add_frame(
            "Mapping", model.mapping, workbook, index_width=[80, 18], column_width=18
        )

    # copy other tabs from source
    if hasattr(model, "_source"):
        _logger.debug("detected source file")

        """merge together with model to also validate these values
        before copying them"""

        # link source file
        xlsx = pd.ExcelFile(model._source)

        # look for interconnectors
        sheet = "Interconnectors"
        if sheet in xlsx.sheet_names:
            # read and write interconnectors
            interconnectors = pd.read_excel(xlsx, sheet, index_col=0)
            add_frame(sheet, interconnectors, workbook, column_width=18)

            _logger.debug("> included '%s' in copy", sheet)

        # look for mpi profiles
        sheet = "MPI Profiles"
        if sheet in xlsx.sheet_names:
            # read and write mpi profiles
            profiles = pd.read_excel(xlsx, sheet)
            add_frame(sheet, profiles, workbook, index=False, column_width=18)

            _logger.debug("> included '%s' in copy", sheet)

    # write workbook
    workbook.close()
