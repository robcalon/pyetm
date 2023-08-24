"""merit order methods"""
from __future__ import annotations

import numpy as np
import pandas as pd

from pyetm.logger import get_modulelogger

from .session import SessionMethods

# get modulelogger
logger = get_modulelogger(__name__)


class MeritOrderMethods(SessionMethods):
    """Merit Order Methods"""

    def _get_merit_configuration(self, include_curves: bool = True):
        """get merit configuration JSON"""

        # request parameters
        params = {"include_curves": str(bool(include_curves)).lower()}
        url = self.make_endpoint_url(endpoint="scenario_id", extra="merit")

        # make request
        configuration = self.session.get(
            url, params=params, content_type="application/json"
        )

        return configuration

    def get_participants(self, subset=None):
        """get particpants from merit configuration"""

        # supported subtypes
        supported = [
            "total_consumption",
            "with_curve",
            "generic",
            "storage",
            "dispatchable",
            "must_run",
            "volatile",
        ]

        # subset all types
        if subset is None:
            subset = supported

        # subset consumer types
        elif (subset == "consumer") | (subset == "consumers"):
            subset = ["total_consumption", "with_curve"]

        # subset flexible types
        elif (subset == "flexible") | (subset == "flexibles"):
            subset = ["generic", "storage"]

        # subset producer types
        elif (subset == "producer") | (subset == "producers"):
            subset = ["dispatchable", "must_run", "volatile"]

        # other keys always in list
        if isinstance(subset, str):
            subset = [subset]

        # convert non list-like to list
        if not isinstance(subset, list):
            subset = list(subset)

        # correct response JSON
        recs = self._get_merit_configuration(False)["participants"]
        recs = [rec for rec in recs if rec.get("type") in subset]

        def correct(rec):
            """null correction in recordings"""
            return {
                k: (v if (v != "null") & (v is not None) else np.nan)
                for k, v in rec.items()
            }

        # correct records to replace null with None
        recs = [correct(rec) for rec in recs]
        frame = pd.DataFrame.from_records(recs, index="key")
        frame = frame.rename_axis(None, axis=0).sort_index()

        # drop curve column
        if "curve" in frame.columns:
            frame = frame.drop(columns="curve")

        return frame

    def get_participant_curves(self):
        """get curves for each participant"""

        # get participants JSON
        response = self._get_merit_configuration()

        # map participants to curve names
        # drops paricipants without curve
        recs = response["participants"]
        cmap = {rec["key"]: rec["curve"] for rec in recs if rec["curve"]}

        # extract curves from response
        curves = response["curves"]
        curves = pd.DataFrame.from_dict(curves)

        def sset_column(key, value):
            """helper to rename subsetted column"""
            return pd.Series(curves[value], name=key)

        # subset curve for each partipant key
        curves = [sset_column(k, v) for k, v in cmap.items()]
        curves = pd.concat(curves, axis=1)

        return curves.sort_index(axis=1)

    def get_dispatchables_bidladder(self):
        """make a bidladder for subset dispatchables.
        returns both marginal costs and installed capacity"""

        # get all dispatchable units
        units = self.get_participants(subset="dispatchable")

        # cap related keys
        keys = ["availability", "number_of_units", "output_capacity_per_unit"]

        # evalaute capacity and specify relevant columns
        units["capacity"] = units[keys].product(axis=1)
        units = units[["marginal_costs", "capacity"]]

        return units.sort_values(by="marginal_costs")
