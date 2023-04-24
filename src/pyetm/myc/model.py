"""myc access"""
from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import datetime
import numpy as np
import pandas as pd

from pyetm import Client
from pyetm.utils import categorise_curves
from pyetm.logger import get_modulelogger, log_exception
from pyetm.optional import import_optional_dependency

_logger = get_modulelogger(__name__)

Carrier = Literal['electricity', 'heat', 'hydrogen', 'methane']

"""externalize hard coded ETM parameters"""

def sort_frame(
    frame: pd.DataFrame,
    axis: int = 0,
    reference: str | None = None
) -> pd.DataFrame:
    """sort frame with reference scenario at start position"""

    # sort columns and get scenarios
    frame = frame.sort_index(axis=axis)
    scenarios = frame.axes[axis].get_level_values(level='SCENARIO')

    # handle reference scenario
    if (reference is not None) & (reference in scenarios):

        # subset reference from frame
        ref = frame.xs(reference, level='SCENARIO', axis=axis,
            drop_level=False)

        # drop reference from frame and apply custom order
        frame = frame.drop(reference, level='SCENARIO', axis=axis)
        frame = pd.concat([ref, frame], axis=axis)

    return frame

class MYCClient():
    """Multi Year Chart Client"""

    @property
    def session_ids(self) -> pd.Series:
        """series of scenario_ids"""
        return self.__session_ids

    @session_ids.setter
    def session_ids(self, session_ids: pd.Series):

        # convert to series
        if not isinstance(session_ids, pd.Series):
            session_ids = pd.Series(session_ids)

        # set uniform index names
        keys = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']
        session_ids.index.names = keys

        # set session ids
        self.__session_ids = session_ids

        # revalidate reference scenario
        if hasattr(self, '_reference'):
            self.reference = self.reference

    @property
    def parameters(self) -> pd.Series:
        """parameters"""
        return self.__parameters

    @parameters.setter
    def parameters(self, parameters: pd.Series):

        # convert to series
        if not isinstance(parameters, pd.Series):
            parameters = pd.Series(parameters)

        # set parameters
        self.__parameters = parameters

    @property
    def gqueries(self) -> pd.Series:
        """gqueries"""
        return self.__gqueries

    @gqueries.setter
    def gqueries(self, gqueries: pd.Series):

        # convert to series
        if not isinstance(gqueries, pd.Series):
            gqueries = pd.Series(gqueries)

        # set gqueries
        self.__gqueries = gqueries

    @property
    def mapping(self) -> pd.DataFrame:
        """mapping"""
        return self.__mapping

    @mapping.setter
    def mapping(self, mapping: pd.DataFrame | None):

        # convert to dataframe
        if (not isinstance(mapping, pd.DataFrame)) & (mapping is not None):
            mapping = pd.DataFrame(mapping)

        # set default index names
        if isinstance(mapping, pd.DataFrame):
            mapping.index.names = ['KEY', 'CARRIER']

        # set mapping
        self.__mapping = mapping

    @property
    def depricated(self) -> list[str]:
        """depricated parameters"""

        # subset depricated parameters
        depricated = [
            'areable_land',
            'buildings_roof_surface_available_for_pv',
            'co2_emission_1990',
            'co2_emission_1990_aviation_bunkers',
            'co2_emission_1990_marine_bunkers',
            'coast_line',
            'number_of_buildings_both',
            'number_of_buildings_present',
            'number_of_inhabitants',
            'number_of_inhabitants_present',
            'number_of_residences',
            'offshore_suitable_for_wind',
            'residences_roof_surface_available_for_pv',
            ]

        return depricated

    @property
    def excluded(self) -> list[str]:
        """excluded parameters"""

        # list of excluded parameters for TYNDP
        excluded = [

            # hidden parameter for HOLON project
            'holon_gas_households_useful_demand_heat_per_person_absolute',

            # issues with regard to min/max ranges and non-demand units
            'efficiency_industry_chp_combined_cycle_gas_power_fuelmix_electricity',
            'efficiency_industry_chp_combined_cycle_gas_power_fuelmix_heat',
            'efficiency_industry_chp_engine_gas_power_fuelmix_electricity',
            'efficiency_industry_chp_engine_gas_power_fuelmix_heat',
            'efficiency_industry_chp_turbine_gas_power_fuelmix_electricity',
            'efficiency_industry_chp_turbine_gas_power_fuelmix_heat',
            'efficiency_industry_chp_ultra_supercritical_coal_electricity',
            'efficiency_industry_chp_ultra_supercritical_coal_heat',
            'efficiency_industry_chp_wood_pellets_electricity',
            'efficiency_industry_chp_wood_pellets_heat',

            # set flh parameters instead
            'flh_of_energy_power_wind_turbine_coastal_user_curve',
            'flh_of_energy_power_wind_turbine_inland_user_curve',
            'flh_of_energy_power_wind_turbine_offshore_user_curve',
            'flh_of_solar_pv_solar_radiation_user_curve',

            # breaking paramter for scenarios
            'capacity_of_industry_metal_flexibility_load_shifting_electricity',
        ]

        return excluded

    @property
    def reference(self) -> str | None:
        """reference scenario key"""
        return self._reference

    @reference.setter
    def reference(self, reference: str | None):

        # validate reference key
        scenarios = self.session_ids.index.unique(level='SCENARIO')
        if (reference not in scenarios) & (reference is not None):
            raise KeyError(f"Invalid reference key: '{reference}'")

        # set reference key
        self._reference = reference

    def __init__(
        self,
        session_ids: pd.Series,
        parameters: pd.Series, gqueries: pd.Series,
        mapping: pd.Series | pd.DataFrame | None = None,
        reference: str | None = None,
        **kwargs
    ):
        """initialisation logic for Client.

        Parameters
        ----------
        session_ids: pd.Series
            Series with session ids.
        parameters: pd.Series
            Series with parameters names to collect and
            corresponding parameter unit.
        gqueries: pd.Series
            Series with gqueries to collect and corresponding
            gquery unit.
        mapping : pd.Series or pd.DataFrame
            Optional mapping for carrier curves output keys.
        reference : str, default None
            Key of reference scenario. This scenario will be
            excluded from the MYC links and will always be
            placed in front when sorting results.

        All key-word arguments are passed directly to the Session
        that is used in combination with the pyETM.client. In this
        module the pyETM.Client uses a Requests Session object as
        backend.

        Keyword Arguments
        -----------------
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

        # set optional parameters
        self.session_ids = session_ids
        self.parameters = parameters
        self.gqueries = gqueries
        self.mapping = mapping
        self.reference = reference

        # set kwargs
        self._kwargs = kwargs

    @classmethod
    def from_excel(cls, filepath: str, **kwargs):
        """initate from excel file with standard structure"""

        # connect to excel file
        xlsx = pd.ExcelFile(filepath)

        # get session ids
        session_ids = pd.read_excel(xlsx, sheet_name='Sessions',
            usecols=[*range(5)], index_col=[*range(4)]).squeeze('columns')

        # get paramters
        parameters = pd.read_excel(xlsx, sheet_name='Parameters',
            usecols=[*range(2)], index_col=[*range(1)]).squeeze('columns')

        # get gqueries
        gqueries = pd.read_excel(xlsx, sheet_name='GQueries',
            usecols=[*range(2)], index_col=[*range(1)]).squeeze('columns')

        # check for optional mapping
        if "Mapping" in xlsx.sheet_names:

            # load mapping
            mapping = pd.read_excel(filepath, sheet_name='Mapping',
                index_col=[*range(2)])

        else:

            # default mapping
            mapping = None

        # intialize model
        model = cls(session_ids=session_ids, parameters=parameters,
            gqueries=gqueries, mapping=mapping, **kwargs)

        # set source in model
        model._source = filepath

        return model

    def _check_for_unmapped_input_parameters(self,
        client: Client) -> pd.Index:

        # get parameters from client side
        parameters = client.scenario_parameters

        # subset external coupling nodes
        key = 'external_coupling'
        subset = parameters.index.str.contains(key, regex=True)

        # drop external coupling nodex
        parameters = parameters.loc[~subset]

        # subset and drop depricated or excluded
        keys = self.depricated + self.excluded
        subset = parameters.index.isin(keys)
        parameters = parameters.loc[~subset]

        # keys not in parameters but in parameters
        missing = parameters[~parameters.index.isin(self.parameters.index)]
        missing = pd.Index(missing.index, name='unmapped keys')

        return missing

    def _make_midx(self,
        midx: tuple | pd.MultiIndex | None = None) -> pd.MultiIndex:
        """helper to handle passed multiindex"""

        # default to all
        if midx is None:
            midx = self.session_ids.index

        # add tuple to list
        if isinstance(midx, tuple):
            midx = [midx]

        # make multiindex
        if not isinstance(midx, pd.MultiIndex):
            midx = pd.MultiIndex.from_tuples(midx)

        return midx

    def get_input_parameters(
        self, midx: tuple | pd.MultiIndex | None = None) -> pd.DataFrame:
        """get input parameters for single scenario"""

        # subset cases of interest
        midx = self._make_midx(midx=midx)
        cases = self.session_ids.loc[midx]

        _logger.info("collecting input parameters")

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                values = []

                # connect scenario and check for unmapped keys
                client.gqueries = list(self.gqueries.index)

                # newset
                warned = False
                unmapped = set()

                # get parameter settings
                for case, scenario_id in cases.items():

                    # log event
                    _logger.debug("> collecting inputs for " +
                        "'%s', '%s', '%s', '%s'", *case)

                    # connect scenario and check for unmapped keys
                    client.scenario_id = scenario_id
                    missing = self._check_for_unmapped_input_parameters(client)

                    if (not missing.empty) & (not warned):
                        _logger.warning("unmapped parameters in scenario(s)")
                        warned = True

                    unmapped.update(set(missing))

                    # reference scenario parameters
                    parameters = client.scenario_parameters

                    # collect irrelevant parameters
                    drop = self.excluded + self.depricated
                    drop = self.parameters.index.isin(drop)

                    # collect relevant parameters
                    keep = self.parameters.index[~drop]
                    keep = parameters.index.isin(keep)

                    # make subset amd append
                    parameters = parameters[keep]
                    values.append(parameters)

                # warn for unmapped keys
                if unmapped:
                    _logger.warn("encountered unmapped parameters: %s",
                        unmapped)

        # handle exception
        except Exception as exc:
            log_exception(exc, logger=_logger)

        # construct frame and handle nulls
        frame = pd.concat(values, axis=1, keys=midx)
        frame = frame.fillna(np.nan)

        # set mapped units as index level
        units = frame.index.map(self.parameters)
        frame = frame.set_index(units, append=True)

        # set names of index levels
        frame.index.names = ['KEY', 'UNIT']
        frame.columns.names = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']

        return sort_frame(frame, axis=1, reference=self.reference)

    def set_input_parameters(
        self, frame: pd.DataFrame) -> None:
        """set input parameters"""

        # convert series to frame
        if isinstance(frame, pd.Series):
            frame = frame.to_frame()

        # convert framelike to frame
        if not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)

        # drop unit from index
        if 'UNIT' in frame.index.names:
            frame = frame.reset_index(level='UNIT', drop=True)

        # ensure dataframe is consistent with session ids
        errors = [midx for midx in frame.columns
                    if midx not in self.session_ids.index]

        # raise for errors
        if errors:
            raise KeyError(f"unknown cases in dataframe: '{errors}'")

        _logger.info("changing input parameters")

        # list external coupling related keys
        key = 'external_coupling'
        external = list(frame.index[frame.index.str.contains(key)])

        # make collection of illegal parameters
        illegal = external + self.excluded + self.depricated
        illegal = frame.index[frame.index.isin(illegal)]

        # trigger warnings for mapped but disabled parameters
        if not illegal.empty:

            # warn for keys
            for key in illegal:
                _logger.warning("excluded '{key}' from upload")

            # drop excluded parameters
            frame = frame.drop(illegal)

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # iterate over cases
                for case, values in frame.items():

                    # log event
                    _logger.debug("> changing input parameters for " +
                        "'%s', '%s', '%s', '%s'", *case)

                    # drop unchanged keys
                    values = values.dropna()

                    # continue if no value specified
                    if values.empty:
                        continue

                    # connect to case and set values
                    client.scenario_id = self.session_ids.loc[case]
                    client.user_values = values

        # handle exception
        except Exception as exc:
            log_exception(exc, logger=_logger)

    def get_hourly_carrier_curves(
        self,
        carrier: str,
        midx: tuple | pd.MultiIndex | None = None,
        mapping: pd.DataFrame | None = None,
        columns: list | None = None,
        include_keys: bool = False,
        invert_sign: bool = False,
    ) -> pd.DataFrame:
        """get hourly carrier curves for scenarios"""

        # lower carrier
        carrier = carrier.lower()

        # subset cases of interest
        midx = self._make_midx(midx=midx)
        cases = self.session_ids.loc[midx]

        # get default mapping
        if (mapping is None) & (self.mapping is not None):

            # lookup carriers in mapping and lower elements
            carriers = list(self.mapping.index.levels[1])
            lcarriers = [carrier.lower() for carrier in carriers]

            # check if carrier is available
            if not carrier in lcarriers:
                raise KeyError("'%s' not specified as carrier in mapping")

            # subset correct carrier
            index = carriers[lcarriers.index(carrier)]
            mapping = self.mapping.xs(index, level=1)

        _logger.info("collecting hourly %s curves", carrier)

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                items = []

                # iterate over cases
                for case, scenario_id in cases.items():

                    # log event
                    _logger.debug("> collecting hourly %s curves for " +
                        "'%s', '%s', '%s', '%s'", carrier, *case)

                    # connect scenario and get curves
                    client.scenario_id = scenario_id

                    # get method for carrier curves
                    attr = f"hourly_{carrier}_curves"
                    curves = getattr(client, attr)

                    # set column name
                    curves.columns.names = ['KEY']

                    # check for categorisation
                    if mapping is not None:

                        # categorise curves
                        curves = categorise_curves(
                            curves, mapping, columns=columns,
                            include_keys=include_keys, invert_sign=invert_sign)

                    # append curves to list
                    items.append(curves)

        # handle exception
        except Exception as exc:
            log_exception(exc, logger=_logger)

        # construct frame for carrier
        frame = pd.concat(items, axis=1, keys=midx)

        return sort_frame(frame, axis=1, reference=self.reference)

    def get_hourly_price_curves(
        self,
        midx: tuple | pd.MultiIndex | None = None,
    ) -> pd.DataFrame:
        """get hourly prices curves"""

        # subset cases of interest
        midx = self._make_midx(midx=midx)
        cases = self.session_ids.loc[midx]

        _logger.info("collecting hourly price curves")

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                items = []

                # iterate over cases
                for case, scenario_id in cases.items():

                    # log event
                    _logger.debug("> collecting hourly price curve for " +
                        "'%s', '%s', '%s', '%s'", *case)

                    # connect scenario and get curves
                    client.scenario_id = scenario_id
                    curves = client.hourly_electricity_price_curve

                    # append curves to list
                    items.append(curves)

        # handle exception
        except Exception as exc:
            log_exception(exc, logger=_logger)

        # construct frame for carrier
        frame = pd.concat(items, axis=1, keys=midx)

        return sort_frame(frame, axis=1, reference=self.reference)

    def get_output_values(
        self,
        midx: tuple | pd.MultiIndex | None = None,
    ) -> pd.DataFrame:
        """get input parameters for single scenario"""

        # subset cases of interest
        midx = self._make_midx(midx=midx)
        cases = self.session_ids.loc[midx]

        _logger.info("collecting gquery results")

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                values = []

                # connect scenario and check for unmapped keys
                client.gqueries = list(self.gqueries.index)

                # get parameter settings
                for case, scenario_id in cases.items():

                    # log event
                    _logger.debug("> collecting gquery results for " +
                        "'%s', '%s', '%s', '%s'", *case)

                    # connect scenario and set gqueries
                    client.scenario_id = scenario_id
                    gqueries = client.gquery_results['future']

                    # append gquery results
                    values.append(gqueries)

        # handle exception
        except Exception as exc:
            log_exception(exc, logger=_logger)

        # construct frame and handle nulls
        frame = pd.concat(values, axis=1, keys=midx)
        frame = frame.fillna(np.nan)

        # set mapped units as index level
        units = frame.index.map(self.gqueries)
        frame = frame.set_index(units, append=True)

        # set names of index levels
        frame.index.names = ['KEY', 'UNIT']
        frame.columns.names = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']

        # fill missing values
        frame = frame.fillna(0)

        return sort_frame(frame, axis=1, reference=self.reference)

    def make_myc_urls(
        self,
        midx: tuple | pd.MultiIndex | None = None,
    ) -> pd.Series:
        """convert session ids excel to myc urls"""

        # default to all
        if midx is None:
            midx = self.session_ids.index

        # add tuple to list
        if isinstance(midx, tuple):
            midx = [midx]

        # make multiindex
        if not isinstance(midx, pd.MultiIndex):
            midx = pd.MultiIndex.from_tuples(midx)

        # subset cases of interest
        cases = self.session_ids.loc[midx]

        if self.reference is not None:

            # drop reference scenario
            if self.reference in cases.index.unique(level='SCENARIO'):
                cases = cases.drop(self.reference, level='SCENARIO', axis=0)

            # no cases dropped
            if cases.empty:

                # warn user
                _logger.warning("Cannot make URLs as model or passed subset "
                    "only contains reference scenarios")

                return pd.Series()

        _logger.info("making MYC URLS")

        levels = "STUDY", "SCENARIO", "REGION"
        grouper = cases.astype(str).groupby(level=levels)

        # convert paths to MYC urls
        urls = "https://myc.energytransitionmodel.com/"
        urls += grouper.apply(lambda x: ",".join(x)) + "/inputs"

        # add title
        urls += "?title=" + urls.index.to_series().str.join("%20")

        return pd.Series(urls, name='URL').sort_index()

    def to_excel(self,
        filepath: str | None = None,
        midx: tuple | pd.MultiIndex | None = None,
        input_parameters: pd.DataFrame | None = None,
        output_values: pd.DataFrame | None = None,
        myc_urls: pd.Series | None = None,
        include_hourly_curves: bool = False,
        include_price_curves: bool = False,
        carriers: Carrier | list[Carrier] | None = None,
        mapping: pd.DataFrame | None = None,
        columns: list[str] | None = None,
        include_keys: bool = False,
        invert_sign: bool = False,
        ) -> None:
        """Export results of model to Excel.

        Parameters
        ----------
        filepath : str, default None
            File path or existing ExcelWriter, defaults
            to filename with current datetime in current
            working directory.
        midx : tuple or pd.MultiIndex, default None
            Tuple or MultiIndex with column names
            of columns to include in file. Defaults
            to all columns.
        input_parameters : pd.DataFrame, default None
            DataFrame to write on the inputs sheet
            of the Excel. Defaults to get_input_values
            method with specified midx.
        output_values : pd.DataFrame, default None
            DataFrame to write on the outputs sheet
            of the Excel. Defaults to get_output_values
            method with specified midx.
        myc_urls : pd.Series, default None
            Series to write on the url sheet
            of the Excel. Default to make_myc_urls
            method with specified midx.
        include_hourly_curves : bool, default False
            Include hourly carrier curves for specified
            carriers in exported result. Defaults to
            exclude carriers.
        include_price_curves : bool, default False
            Include hourly price curves for the
            selected session ids. Defaults to exclude
            the price curves.
        carriers : str | list, default None
            Carrier of list of carriers to export when
            including hourlu carrier curves. Defaults
            to include all carriers.
        mapping : DataFrame, default None
            DataFrame with mapping of ETM keys and carrier in a multiindex
            and mapping values in columns. Defaults to mapping passed on
            initialisation or is excluded when not passed on model
            initialisation.
        columns : list, default None
            List of column names and the order of mappers that will
            be included in the applied mapping. Defaults to include all
            columns in applied mapping.
        include_keys : bool, default False
            Include the original ETM keys in the resulting mapping.
        invert_sign : bool, default False
            Inverts sign convention where demand is denoted with
            a negative sign. Demand will be denoted with a positve
            value and supply with a negative value."""

        # pylint: disable=C0415
        # Due to optional import

        from pathlib import Path
        from pyetm.utils.excel import add_frame, add_series

        if TYPE_CHECKING:
            # import xlswriter
            import xlsxwriter

        else:
            # import optional dependency
            xlsxwriter = import_optional_dependency('xlsxwriter')

        # pylint: enable=C0415

        # supported carriers
        all_carriers = ['electricity', 'heat', 'hydrogen', 'methane']

        # default carriers
        if carriers is None:
            carriers = all_carriers

        # add string to list
        if isinstance(carriers, str):
            carriers = [carriers]

        # check passed carriers
        for carrier in carriers:
            if carrier.lower() not in carriers:
                raise ValueError("carrier '{carrier}' not supported")

        # make filepath
        if filepath is None:

            # default filepath
            now = datetime.datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + '.xlsx')

        # check filepath
        if not Path(filepath).parent.exists:
            raise FileNotFoundError("Path to file does not exist: '{filepath}'")

        # create workbook
        workbook = xlsxwriter.Workbook(str(filepath))

        # default inputs
        if input_parameters is None:
            input_parameters = self.get_input_parameters(midx=midx)

        # add inputs to workbook
        add_frame('INPUT_PARAMETERS', input_parameters, workbook,
            index_width=[80, 18], column_width=18)

        # default outputs
        if output_values is None:
            output_values = self.get_output_values(midx=midx)

        # add outputs to workbook
        add_frame('OUTPUT_VALUES', output_values, workbook,
            index_width=[80, 18], column_width=18)

        """Allow for modified carrier curves as well"""

        # iterate over carriers
        if include_hourly_curves:
            for carrier in carriers:

                # get carrier curves
                curves = self.get_hourly_carrier_curves(carrier,
                    midx=midx, mapping=mapping, columns=columns,
                    include_keys=include_keys, invert_sign=invert_sign)

                # add to excel
                name = carrier.upper()
                add_frame(name, curves, workbook, column_width=18)

        """Allow for modified price curves as well"""

        # include price curves
        if include_price_curves:

            # get and add hourly price curves
            curves = self.get_hourly_price_curves(midx=midx)
            add_frame('EPRICE', curves, workbook, column_width=18)

        # default urls
        if myc_urls is None:
            myc_urls = self.make_myc_urls(midx=midx)

        # add urls to workbook
        if not myc_urls.empty:
            add_series('ETM_URLS', myc_urls, workbook,
                index_width=18, column_width=80)

        # write workbook
        workbook.close()

        _logger.info("exported results to '%s'", filepath)
