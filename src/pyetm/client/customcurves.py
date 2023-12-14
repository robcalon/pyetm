"""custom curve methods"""
from __future__ import annotations
import functools

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.utils.general import bool_to_json

from .session import SessionMethods

# get modulelogger
logger = get_modulelogger(__name__)


class CustomCurveMethods(SessionMethods):
    """Custom Curve Methods"""

    def _get_overview(
        self, include_unattached: bool = False, include_internal: bool = False
    ) -> pd.DataFrame:
        """fetch custom curve descriptives"""

        # newdict
        params: dict[str, str] = {}

        # include unattached
        if include_unattached is True:
            params["include_unattached"] = bool_to_json(include_unattached)

        # include internal
        if include_internal is True:
            params["include_internal"] = bool_to_json(include_internal)

        # make url
        url = self.make_endpoint_url("custom_curves")

        # get custom curves
        records = self.session.get(url, params=params, content_type="application/json")

        # check for response
        if bool(records) is True:
            ccurves = pd.DataFrame.from_records(records, index="key")

            # format datetime column
            if "date" in ccurves.columns:
                ccurves["date"] = pd.to_datetime(ccurves["date"])

        else:
            # return empty frame
            ccurves = pd.DataFrame()

        return ccurves

    def get_custom_curve_keys(
        self, include_unattached: bool = False, include_internal: bool = False
    ) -> list[str]:
        """get all custom curve keys"""
        return self._get_overview(include_unattached, include_internal).index.to_list()

    def get_custom_curve_settings(
        self, include_unattached: bool = False, include_internal: bool = False
    ) -> pd.DataFrame:
        """show overview of custom curve settings"""

        # get relevant keys
        params = include_unattached, include_internal
        keys = self.get_custom_curve_keys(*params)

        # empty frame without returned keys
        if not keys:
            return pd.DataFrame()

        # reformat overrides
        ccurves = self._get_overview(*params).copy()
        ccurves["overrides"] = ccurves["overrides"].apply(len)

        # drop messy stats column
        if "stats" in ccurves.columns:
            ccurves = ccurves[ccurves.columns.drop("stats")]

        # drop unattached keys
        if not include_unattached:
            ccurves = ccurves.loc[ccurves["attached"]]
            ccurves = ccurves.drop(columns="attached")

        return ccurves.sort_index()

    def get_custom_curve_user_value_overrides(
        self, include_unattached: bool = False, include_internal: bool = False
    ):
        """get overrides of user value keys by custom curves"""

        # subset and explode overrides
        cols = ["overrides", "attached"]

        # get overview curves
        params = include_unattached, include_internal
        overview = self._get_overview(*params).copy()

        # review overview
        if overview.empty:
            return overview

        # explode and drop na
        overrides = overview[cols].explode("overrides")
        overrides = overrides.dropna()

        # reset index
        overrides = overrides.reset_index()
        overrides.columns = pd.Index(["override_by", "user_value_key", "active"])

        # set index
        overrides = overrides.set_index("user_value_key")

        # subset active
        if not include_unattached:
            return overrides.loc[overrides["active"]]

        return overrides

    @property
    def custom_curves(self) -> pd.Series[Any] | pd.DataFrame:
        """fetch custom curves"""
        return self.get_custom_curves()

    @custom_curves.setter
    def custom_curves(self, ccurves: pd.Series[Any] | pd.DataFrame | None):
        """set custom curves without option to set a name"""

        # upload ccurves
        if ccurves:
            self.set_custom_curves(ccurves)

        # delete all ccurves
        if ccurves is None:
            self.delete_custom_curves()

    # consider moving validation to endpoint
    def validate_ccurve_key(self, key: str):
        """check if key is valid ccurve"""

        # check if key in ccurve index
        if str(key) not in self.get_custom_curve_keys(
            include_unattached=True, include_internal=True
        ):
            raise KeyError(f"'{key}' is not a valid custom curve key")

    @functools.lru_cache(maxsize=1)
    def get_custom_curves(
        self, keys: str | Iterable[str] | None = None
    ) -> pd.Series[Any] | pd.DataFrame:
        """get custom curve"""

        # get all attached keys
        attached = self.get_custom_curve_keys(False, True)

        # handle single key
        if isinstance(keys, str):
            keys = [keys]

        # default to all attached keys
        if keys is None:
            keys = attached

        # warn user
        if not keys:
            logger.info("attempting to retrieve custom curves without any attached")

        # warn user
        for key in set(keys).symmetric_difference(attached):
            logger.info(
                "attempting to retrieve '%s' while custom curve not attached", key
            )

        # get curves
        curves: list[pd.Series[Any]] = []
        for key in set(keys).intersection(attached):
            # validate key
            self.validate_ccurve_key(key)

            # make request
            url = self.make_endpoint_url(endpoint="custom_curves", extra=key)
            buffer = self.session.get(url, content_type="text/csv")

            # append as series
            curves.append(pd.read_csv(buffer, header=None, names=[key]).squeeze(axis=1))

        return pd.concat(curves, axis=1).squeeze(axis=1)

    def set_custom_curves(
        self,
        ccurves: pd.Series[Any] | pd.DataFrame,
        filenames: str | Iterable[str | None] | Mapping[str, str] | None = None,
    ) -> None:
        """upload custom curves and delete curves for keys that are not
        present in the uploaded custom curves."""

        # get all attached keys
        attached = self.get_custom_curve_keys(False, True)

        # delete keys that are not reuploaded.
        if attached:
            # transform series
            if isinstance(ccurves, pd.Series):
                ccurves = ccurves.to_frame()

            # delete keys
            self.delete_custom_curves(
                keys=set(ccurves.columns).symmetric_difference(attached)
            )

        # upload custom curves
        self.upload_custom_curves(ccurves, filenames=filenames)

    def upload_custom_curves(
        self,
        ccurves: pd.Series[Any] | pd.DataFrame,
        filenames: str | Iterable[str | None] | Mapping[str, str] | None = None,
    ) -> None:
        """upload custom curves without deleting curves for keys that are
        not present in the uploaded custom curves."""

        # handle ccurves
        if isinstance(ccurves, pd.Series):
            ccurves = ccurves.to_frame()

        # convert single file names or None to iterable
        if isinstance(filenames, str) or (filenames is None):
            filenames = [filenames for _ in ccurves.columns]

        # convert iterable to mapping
        if isinstance(filenames, Iterable):
            # check for lenght mismatches
            if len(list(filenames)) != len(ccurves.columns):
                raise ValueError("lenght mismatch between ccurves and file names")

            # convert to mapping
            filenames = dict(zip(ccurves.columns, list(filenames)))

        # upload columns sequentually
        for key, curve in ccurves.items():
            # validate key
            key = str(key)
            self.validate_ccurve_key(key)

            # check curve length
            if not len(curve) == 8760:
                raise ValueError(f"ccurve '{key}' must contain 8760 entries")

            # reset period / datetime index
            if not isinstance(curve.index, pd.RangeIndex):
                curve = curve.reset_index(drop=True)

            # make request
            url = self.make_endpoint_url(endpoint="custom_curves", extra=key)
            self.session.upload(url, curve.reset_index(drop=True), filename=filenames[key])

        # reset session
        self._reset_cache()

    def delete_custom_curves(self, keys: str | Iterable[str] | None = None) -> None:
        """delete custom curves"""

        # get all attached keys
        attached = self.get_custom_curve_keys(False, True)

        # handle single key
        if isinstance(keys, str):
            keys = [keys]

        # default to all attached keys
        if keys is None:
            keys = attached

        # warn user
        if not keys:
            logger.info("attempting to unattach custom curves without any attached")

        # warn user
        for key in set(keys).symmetric_difference(attached):
            logger.info(
                "attempting to remove '%s' while custom curve already unattached", key
            )

        # delete curves
        for key in set(keys).intersection(attached):
            # validate key
            self.validate_ccurve_key(key)

            # make request
            url = self.make_endpoint_url(endpoint="custom_curves", extra=key)
            self.session.delete(url)

        # reset cache
        self._reset_cache()
