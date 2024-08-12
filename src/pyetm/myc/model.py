"""Updated MYCClient"""

from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import get_args, overload, Hashable, Literal, Iterable, Sequence, TypedDict

import logging

from typing_extensions import NotRequired

import xlsxwriter
import pandas as pd

from pyetm import Client
from pyetm.types import Carrier
from pyetm.utils.general import iterable_to_str
from pyetm.utils.url import make_myc_url, set_url_parameters
from pyetm.utils.excel import add_frame, add_series

from .pool import ClientPool

pd.set_option('future.no_silent_downcasting', True)

ScenarioSlice = Hashable | Sequence[Hashable] | pd.MultiIndex | pd.Series
logger = logging.getLogger(__name__)

class ExcelSheetMapping(TypedDict):
    """Sheet mapping for Excel-based configurations"""
    scenarios: NotRequired[str]
    parameters: NotRequired[str]
    gqueries: NotRequired[str]

@dataclass
class _ExcelSheetMapping:
    """Defaults names for ExcelSheetMapping"""
    scenarios: str = 'scenarios'
    parameters: str = 'parameters'
    gqueries: str = 'gqueries'

def validate_carrier(carrier: Carrier) -> Carrier:
    """validate if carrier is supported"""
    if carrier not in get_args(Carrier):
        raise ValueError(f"Unsupported carrier: {carrier}")
    return carrier

def validate_carrier_sequence(carriers: Carrier | Sequence[Carrier]) -> list[Carrier]:
    """validate if carrier sequence contains supported carrier"""

    # handle single carrier
    if isinstance(carriers, str):
        carriers = [carriers]

    # subset errors
    errors = set(car for car in carriers if car not in get_args(Carrier))
    if errors:
        raise ValueError(f"Unsupported carriers in sequence: {iterable_to_str(errors)}")

    return list(carriers)

class MYCClient:
    """Multi Year Chart Client"""

    @property
    def myc_url(self) -> str:
        """specifies URL that points to mutli-year charts."""
        return self._myc_url

    @myc_url.setter
    def myc_url(self, url: str | None):

        if url is None:
            # check for default engine
            with Client(**self._kwargs) as client:
                default_engine = client.connected_to_default_engine

            # pass default engine myc URL
            if default_engine is True:
                url = "https://myc.energytransitionmodel.com/"

            else:
                # raise for missing myc URL
                raise ValueError(
                    "must specify the related "
                    "custom myc_url for the specified custom engine_url."
                )

        self._myc_url = str(url)

    @property
    def session_ids(self) -> pd.Series:
        """series of scenario_ids"""
        return self._session_ids

    @session_ids.setter
    def session_ids(self, session_ids: pd.Series):

        # convert to series
        if not isinstance(session_ids, pd.Series):
            session_ids = pd.Series(session_ids)

        # set uniform index names
        keys = ["study", "scenario", "region", "year"]
        session_ids.index.names = keys

        # set session ids
        self._session_ids = session_ids

        # revalidate reference scenario
        if hasattr(self, "_reference"):
            self.reference = self.reference

    @property
    def parameters(self) -> pd.Series[str] | None:
        """parameters"""
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: pd.Series | Sequence[str] | None):

        # convert to series
        if parameters is not None:
            if not isinstance(parameters, pd.Series):
                parameters = pd.Series(parameters)

        # set parameters
        self._parameters = parameters

    @property
    def gqueries(self) -> pd.Series[str] | None:
        """gqueries"""
        return self._gqueries

    @gqueries.setter
    def gqueries(self, gqueries: pd.Series | Sequence[str] | None):

        # convert to series
        if gqueries is not None:
            if not isinstance(gqueries, pd.Series):
                gqueries = pd.Series(gqueries)

        # set gqueries
        self._gqueries = gqueries

    @property
    def reference(self) -> str | None:
        """reference scenario key"""
        return self._reference

    @reference.setter
    def reference(self, reference: str | None):
        # validate reference key
        scenarios = self.session_ids.index.unique(level="scenario")
        if (reference not in scenarios) & (reference is not None):
            raise KeyError(f"Invalid reference key: '{reference}'")

        # set reference key
        self._reference = reference

    @property
    def pool(self) -> ClientPool:
        """pool"""
        return self._pool

    @pool.setter
    def pool(self, pool: int | ClientPool | None):

        # defeault pool
        pool = pool if pool else 3
        if isinstance(pool, int):
            pool = ClientPool(maxsize=pool, **self._kwargs)

        self._pool = pool

    def __init__(
        self,
        session_ids: pd.Series,
        parameters: pd.Series | None = None,
        gqueries: pd.Series | None = None,
        reference: str | None = None,
        myc_url: str | None = None,
        pool: int | ClientPool | None = None,
        **kwargs,
    ):
        """initialisation logic for Client.

        Parameters
        ----------
        session_ids: pd.Series
            Series with session ids.
        parameters: pd.Series, default None
            Series with parameters names to collect and
            corresponding parameter unit.
        gqueries: pd.Series, default None
            Series with gqueries to collect and corresponding
            gquery unit.
        reference : str, default None
            Key of reference scenario. This scenario will be
            excluded from the MYC links and will always be
            placed in front when sorting results.
        myc_url : str, default None
            Specify URL that points to ETM MYC, default to public
            multi-year charts.
        pool : int or ClientPool, default None
            Maximum size of initiated client pool or ClientPool
            instance. Kwargs will be ignored if a ClientPool instance
            is passed.

        All key-word arguments are passed directly to the Session
        that is used in combination with the pyetm.client. In this
        module the pyetm.Client uses a Requests Session object by default.

        Optional Kwargs
        ---------------
        base_url: str, default None
            Base url to which the session connects, all request urls
            will be merged with the base url to create a destination.
        proxies: dict, default None
            Dictionary mapping protocol or protocol and
            hostname to the URL of the proxy.
        stream: boolean, default False
            Whether to immediately download the response content.
        verify: boolean or string, default True
            Either a boolean, in which case it controls whether we verify
            the server's TLS certificate, or a string, in which case it must
            be a path to a CA bundle to use. When set to False, requests will
            accept any TLS certificate presented by the server, and will ignore
            hostname mismatches and/or expired certificates, which will make
            your application vulnerable to man-in-the-middle (MitM) attacks.
            Setting verify to False may be useful during local development or
            testing.
        cert: string or tuple, default None
            If string; path to ssl client cert file (.pem).
            If tuple; ('cert', 'key') pair."""

        # set kwargs
        self._source = None
        self._kwargs = kwargs

        # set parameters
        self.pool = pool
        self.session_ids = session_ids
        self.parameters = parameters
        self.gqueries = gqueries
        self.reference = reference
        self.myc_url = myc_url

    @classmethod
    def from_excel(
        cls,
        filepath: str,
        reference: str | None = None,
        myc_url: str | None = None,
        sheet_mapping: ExcelSheetMapping | None = None,
        pool: int | ClientPool | None = None,
        **kwargs,
    ):
        """initate from excel file with standard structure

        Parameters
        ----------
        filepath : str
            Path to excel file that is red.
        reference : str, default None
            Key of reference scenario. This scenario will be
            excluded from the MYC links and will always be
            placed in front when sorting results.
        myc_url : str, default None
            Specify URL that points to ETM MYC, default to public
            multi-year charts.
        sheet_mapping : dict, default None
            Optional mapping of sheet names.
        pool : int or ClientPool, default None
            Maximum size of initiated client pool or ClientPool
            instance. Kwargs will be ignored if a ClientPool instance
            is passed.

        All key-word arguments are passed directly to the Session that is
        used in combination with the pyetm.client. In this module the
        pyetm.Client uses a Requests Session object by default."""

        def read_sheet(
            xlsx: pd.ExcelFile,
            sheet_name: str,
            required: bool = True,
            **kwargs
        ) -> pd.Series:
            """read list items"""

            if not sheet_name in xlsx.sheet_names:
                if required:
                    raise ValueError(f"Could not load required sheet '{sheet_name}' from {xlsx.io}")
                logger.warning("Could not load optional sheet '%s' from '%s'", sheet_name, xlsx.io)
                return pd.Series(name=sheet_name, dtype=str)

            values = pd.read_excel(xlsx, sheet_name, **kwargs).squeeze(axis=1)
            if not isinstance(values, pd.Series):
                raise TypeError("Unexpected Outcome")

            return values.rename(sheet_name)

        # default sheet mapping
        if sheet_mapping is None:
            sheet_mapping = ExcelSheetMapping()
        mapping = _ExcelSheetMapping(**sheet_mapping)

        # connect to excel file
        xlsx = pd.ExcelFile(filepath)

        with pd.ExcelFile(filepath) as xlsx:

            # load session ids
            session_ids = read_sheet(
                xlsx, mapping.scenarios, usecols=list(range(5)), index_col=list(range(4))
            )

            # load parameters and gqueries
            parameters = read_sheet(xlsx, mapping.parameters, required=False, usecols=[0])
            gqueries = read_sheet(xlsx, mapping.gqueries, required=False, usecols=[0])

        # intialize model
        model = cls(
            session_ids=session_ids,
            parameters=parameters,
            gqueries=gqueries,
            reference=reference,
            myc_url=myc_url,
            pool=pool,
            **kwargs,
        )

        return model

    def slice_cases(self, scenarios: ScenarioSlice | None = None) -> pd.Series[int]:
        """slice cases"""

        # default to all cases
        if scenarios is None:
            if not isinstance(self.session_ids.index, pd.MultiIndex):
                raise TypeError("wrong index type")
            scenarios = self.session_ids.index

        if isinstance(scenarios, pd.Series):
            if not isinstance(scenarios.index, pd.MultiIndex):
                raise TypeError("wrong index type")
            scenarios = scenarios.index

        if not isinstance(scenarios, pd.MultiIndex):
            if not isinstance(scenarios, Sequence):
                scenarios = [scenarios]
            scenarios = pd.MultiIndex.from_tuples(scenarios)

        return self.session_ids.loc[scenarios]

    def get_parameters(
        self,
        parameters: Sequence[str] | pd.Series | None = None,
        scenarios: ScenarioSlice | None = None,
        exclude: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """get parameters"""

        # collect parameters and scenarios
        parameters = parameters if parameters else self.parameters
        scenarios = self.slice_cases(scenarios=scenarios)

        return self.pool.get_parameters(scenarios, parameters, exclude=exclude, **kwargs)

    def set_parameters(
        self,
        parameters: pd.Series | pd.DataFrame,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> None:
        """set parameters"""

        # collect scenarios
        scenarios = self.slice_cases(scenarios=scenarios)

        if isinstance(parameters, pd.Series):
            parameters = parameters.to_frame()

        if 'unit' in parameters.index.names:
            parameters = parameters.reset_index(level='unit', drop=True)

        # ensure dataframe is consistent with session ids
        errors = [scenarios for scenarios in parameters.columns if scenarios not in self.session_ids.index]
        if errors:
            raise KeyError(f"unknown cases in dataframe: '{errors}'")

        self.pool.set_parameters(scenarios, parameters, **kwargs)

    def upload_custom_curves(
        self,
        ccurves: pd.Series | pd.DataFrame,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> None:
        """upload custom curves"""
        scenarios = self.slice_cases(scenarios=scenarios)
        self.pool.upload_custom_curves(ccurves=ccurves, scenarios=scenarios, **kwargs)

    def delete_custom_curves(
        self,
        keys: str | Iterable[str] | None = None,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> None:
        """delete custom curves"""
        scenarios = self.slice_cases(scenarios=scenarios)
        self.pool.delete_custom_curves(keys=keys, scenarios=scenarios, **kwargs)

    def set_custom_curves(
        self,
        ccurves: pd.Series | pd.DataFrame,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> None:
        """set custom curves"""
        scenarios = self.slice_cases(scenarios=scenarios)
        self.pool.set_custom_curves(ccurves=ccurves, scenarios=scenarios, **kwargs)

    def get_gqueries(
        self,
        gqueries: Iterable[str] | pd.Series | None = None,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> pd.DataFrame:
        """get gqueries"""

        # collect gqueries
        gqueries = gqueries if gqueries else self.gqueries
        if gqueries is None:
            raise ValueError("no gqueries specified")

        # collect scenarios
        scenarios = self.slice_cases(scenarios=scenarios)

        return self.pool.get_gqueries(scenarios=scenarios, gqueries=gqueries, **kwargs)

    def get_price_curves(
        self,
        carriers: Carrier | Iterable[Carrier] | None = None,
        scenarios: ScenarioSlice | None = None,
        **kwargs
    ) -> pd.DataFrame:
        """get hourly price curves"""

        # collect scenarios
        scenarios = self.slice_cases(scenarios=scenarios)
        return self.pool.get_price_curves(scenarios, carriers, **kwargs)

    def get_carrier_curves(
        self,
        carrier: Carrier,
        scenarios: ScenarioSlice | None = None,
        invert_sign_convention: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """get hourly carrier curves"""

        # collect scenarios
        scenarios = self.slice_cases(scenarios=scenarios)
        return self.pool.get_carrier_curves(scenarios, carrier, invert_sign_convention, **kwargs)

    def make_myc_urls(
        self,
        scenarios: ScenarioSlice | None = None,
        path: str | None = None,
        params: dict[str, str] | None = None,
        add_title: bool = True,
    ) -> pd.Series:
        """convert session ids excel to myc urls"""

        scenarios = self.slice_cases(scenarios=scenarios)

        # drop reference scenario
        if self.reference is not None:
            if self.reference in scenarios.index.unique(level="scenario"):
                scenarios = scenarios.drop(self.reference, level="scenario", axis=0)

            # no cases dropped
            if scenarios.empty:
                logger.warning(
                    "Cannot make URLs as model or passed subset "
                    "only contains reference scenarios"
                )
                return pd.Series()

        # group scenario ids
        levels = "study", "scenario", "region"
        urls = scenarios.astype(str).groupby(level=levels)

        # make urls
        urls = urls.apply(
            lambda sids: make_myc_url(
                url=self.myc_url, scenario_ids=sids, path=path, params=params
            )
        )

        # add title and parameters
        if bool(add_title) is True:
            for idx, url in urls.items():
                params = {"title": " ".join(map(str, idx))} # pyright: ignore
                urls.at[idx] = set_url_parameters(url, params=params)

        return pd.Series(urls, name="url").sort_index()

    @overload
    def convert_to_long(
        self,
        frame: pd.DataFrame,
        as_frame: Literal[False] = False
    ) -> pd.Series:
        pass

    @overload
    def convert_to_long(
        self,
        frame: pd.DataFrame,
        as_frame: Literal[True] = True
    ) -> pd.DataFrame:
        pass

    def convert_to_long(
        self,
        frame: pd.DataFrame,
        as_frame: bool = False
    ) -> pd.Series | pd.DataFrame:
        """transform frame based on selected mode"""

        # convert header to foreign key
        obj = frame.copy(deep=True)
        mapper = dict(zip(self.session_ids.index, pd.RangeIndex(len(self.session_ids))))

        # map header to scenario id
        obj.columns = obj.columns.map(mapper)
        obj.columns.names = ['scenario_id']

        # set reorder order before stacking
        order = [-1, *range(obj.index.nlevels)]

        # stack and reorder
        obj = obj.stack(level='scenario_id')
        obj = obj.reorder_levels(order=order)

        # sort index with new level order
        obj = obj.sort_index()

        # validate outcome
        if not isinstance(obj, pd.Series):
            raise TypeError(f"Expected Series instead of {type(obj)}.")

        # rename series
        obj = obj.rename('value')

        if as_frame is True:
            obj = obj.reset_index()

        return obj

    def write_frame_as_parquet_table(
        self,
        frame: pd.DataFrame,
        name: str,
        dirpath: Path,
        dtype: type | dict[str, type] | None = None
    ) -> None:
        """df"""

        # convert frame
        filename = dirpath.joinpath(f'{name}.parquet')
        frame = self.convert_to_long(frame, as_frame=True)

        # convert dtype(s)
        if dtype is not None:
            frame = frame.astype(dtype)

        return frame.to_parquet(filename)

    def to_parquet(
        self,
        dirpath: str | PathLike | None = None,
        scenarios: ScenarioSlice | None = None,
        parameters: bool = True,
        gqueries: bool = True,
        carrier_curves: bool = False,
        price_curves: bool = False,
        carriers: Carrier | Sequence[Carrier] | None = None,
        exclude: bool = False,
        invert_sign_convention: bool = False,
    ) -> None:
        """Export results of model to Excel.

        Parameters
        ----------
        dirpath : str, default None
            Directory path, defaults to a datetime folder in
            the current working directory.
        scenarios : tuple or pd.MultiIndex, default None
            Tuple or MultiIndex with column names
            of columns to include in file. Defaults
            to all columns.
        input_parameters : bool or pd.DataFrame, default True
            Include input parameters in export. Can be overwritten with
            custom parameters by passing a DataFrame instead.
        output_values : bool or pd.DataFrame, default True
            Include gquery results in export. Can be overwritten with
            custom curves by passing a DataFrame instead.
        carrier_curves : bool, default False
            Include hourly carrier curves for specified
            carriers in exported result. Defaults to
            exclude carriers.
        price_curves : bool, default False
            Include hourly price curves for the
            selected session ids. Defaults to exclude
            the price curves.
        carriers : str | list, default None
            Carrier of list of carriers to export when
            including hourly carrier curves. Defaults
            to include all carriers.
        exclude : str | list, default None
            Parameter to exclude from export. Defaults to include
            all active parameters.
        invert_sign_convention : bool, default False
            Inverts sign convention where demand is denoted with
            a negative sign. Demand will be denoted with a positve
            value and supply with a negative value."""

        # default carriers
        if carriers is None:
            carriers = get_args(Carrier)

        # validate carriers
        carriers = validate_carrier_sequence(carriers)

        # default dirpath
        if dirpath is None:
            now = datetime.now().strftime("%Y%m%d%H%M")
            dirpath = Path.cwd().joinpath(now)

        # convert dirpath to Path-object
        if not isinstance(dirpath, Path):
            dirpath = Path(dirpath)

        # create directory
        dirpath.mkdir(parents=False, exist_ok=True)

        # create unique scenario_ids
        sids = self.session_ids.reset_index()
        sids.insert(loc=0, column='scenario_id', value=sids.index)

        # export scenario ids
        sids.columns = sids.columns.str.lower()
        sids.to_parquet(dirpath.joinpath('scenarios.parquet'))

        # write parameters
        if parameters is not False:
            frame = self.get_parameters(scenarios=scenarios, exclude=exclude)
            mask = frame.index.isin({'bool', 'literal'}, level='unit')

            self.write_frame_as_parquet_table(frame[~mask], 'parameters', dirpath)
            self.write_frame_as_parquet_table(frame[mask], 'settings', dirpath, dtype={'value': str})

        # write gqueries
        if gqueries is not False:
            if self.gqueries is not None:
                frame = self.get_gqueries(scenarios=scenarios)
                self.write_frame_as_parquet_table(frame, 'gqueries', dirpath)

        # write price curves
        if price_curves is not False:
            frame = self.get_price_curves(carriers=carriers, scenarios=scenarios)
            self.write_frame_as_parquet_table(frame, 'prices', dirpath)

        # write carrier curves
        if carrier_curves is not False:
            for carrier in carriers:
                frame = self.get_carrier_curves(
                    carrier=carrier,
                    scenarios=scenarios,
                    invert_sign_convention=invert_sign_convention
                )
                self.write_frame_as_parquet_table(frame, carrier, dirpath)

    def to_excel(
        self,
        filepath: str | PathLike | None = None,
        scenarios: ScenarioSlice | None = None,
        parameters: bool = True,
        gqueries: bool = True,
        carrier_curves: bool = False,
        price_curves: bool = False,
        myc_urls: bool = True,
        carriers: Carrier | Sequence[Carrier] | None = None,
        exclude: bool = False,
        invert_sign_convention: bool = False,
    ) -> None:
        """Export results of model to Excel.

        Parameters
        ----------
        filepath : str, default None
            File path or existing ExcelWriter, defaults
            to filename with current datetime in current
            working directory.
        scenarios : tuple or pd.MultiIndex, default None
            Tuple or MultiIndex with column names
            of columns to include in file. Defaults
            to all columns.
        input_parameters : bool or pd.DataFrame, default True
            Include input parameters in export. Can be overwritten with
            custom parameters by passing a DataFrame instead.
        output_values : bool or pd.DataFrame, default True
            Include gquery results in export. Can be overwritten with
            custom curves by passing a DataFrame instead.
        carrier_curves : bool, default False
            Include hourly carrier curves for specified
            carriers in exported result. Defaults to
            exclude carriers.
        price_curves : bool, default False
            Include hourly price curves for the
            selected session ids. Defaults to exclude
            the price curves.
        carriers : str | list, default None
            Carrier of list of carriers to export when
            including hourly carrier curves. Defaults
            to include all carriers.
        exclude : str | list, default None
            Parameter to exclude from export. Defaults to include
            all active parameters.
        invert_sign_convention : bool, default False
            Inverts sign convention where demand is denoted with
            a negative sign. Demand will be denoted with a positve
            value and supply with a negative value."""

        # default carriers
        if carriers is None:
            carriers = get_args(Carrier)

        carriers = validate_carrier_sequence(carriers)

        # default filepath
        if filepath is None:
            now = datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + ".xlsx")

        # convert dirpath to Path-object
        if not isinstance(filepath, Path):
            filepath = Path(filepath)

        # check filepath
        if not Path(filepath).parent.exists:
            raise FileNotFoundError(f"Path to file does not exist: '{filepath}'")

        # create workbook
        workbook = xlsxwriter.Workbook(str(filepath))

        # write parameters
        if parameters is not False:
            frame = self.get_parameters(scenarios=scenarios, exclude=exclude)
            add_frame("PARAMETERS", frame, workbook, index_width=[80, 18], column_width=18)

        # write gqueries
        if gqueries is not False:
            frame = self.get_gqueries(scenarios=scenarios)
            add_frame("GQUERIES", frame, workbook, index_width=[80, 18], column_width=18)

        # write price curves
        if price_curves is not False:
            frame = self.get_price_curves(carriers=carriers, scenarios=scenarios)

            # unstack frame
            frame = frame.unstack(level="carrier")
            if not isinstance(frame, pd.DataFrame):
                frame = frame.to_frame()

            add_frame("PRICES", frame, workbook, column_width=18)

        # write carrier curves
        if carrier_curves is not False:
            for carrier in carriers:
                frame = self.get_carrier_curves(
                    carrier=carrier,
                    scenarios=scenarios,
                    invert_sign_convention=invert_sign_convention
                )

                # unstack frame
                frame = frame.unstack(level=("carrier", "curve"))
                if not isinstance(frame, pd.DataFrame):
                    frame = frame.to_frame()

                add_frame(carrier.upper(), frame, workbook, column_width=18)

        if myc_urls is not False:
            series = self.make_myc_urls(scenarios=scenarios)

            if not series.empty:
                add_series("ETM_URLS", series, workbook, index_width=18, column_width=80)

        # write workbook
        workbook.close()

        logger.info("exported results to '%s'", filepath)
