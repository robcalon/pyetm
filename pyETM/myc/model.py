from __future__ import annotations

import datetime

import numpy as np
import pandas as pd

from pathlib import Path
from typing import TYPE_CHECKING

from pyETM import Client
from pyETM.logger import get_modulelogger, export_logfile
from pyETM.optional import import_optional_dependency

_logger = get_modulelogger(__name__)

"""externalize hard coded ETM parameters"""

def sort_frame(frame: pd.DataFrame):
    """sort frame with reference scenario at start position"""

    # sort columns
    frame = frame.sort_index(axis=1)
    
    # check for reference scenario
    scenarios = frame.columns.get_level_values(level='SCENARIO')
    if 'Reference' in scenarios.unique():

        # subset reference and drop from frame
        ref = frame.xs('Reference', level='SCENARIO', axis=1, drop_level=False)
        frame = frame.drop(ref.columns, axis=1)

        # merge back in correct order
        frame = ref.join(frame)

    return frame


class Model():

    @property
    def session_ids(self) -> pd.Series:
        return self.__session_ids

    @session_ids.setter
    def session_ids(self, session_ids):

        # convert to series        
        if not isinstance(session_ids, pd.Series):
            session_ids = pd.Series(session_ids)
        
        # set uniform index names
        keys = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']
        session_ids.index.names = keys

        # set session ids
        self.__session_ids = session_ids

    @property
    def parameters(self) -> pd.Series:
        return self.__parameters

    @parameters.setter
    def parameters(self, parameters):

        # convert to series        
        if not isinstance(parameters, pd.Series):
            parameters = pd.Series(parameters)
        
        # set parameters
        self.__parameters = parameters

    @property
    def gqueries(self) -> pd.Series:
        return self.__gqueries
    
    @gqueries.setter
    def gqueries(self, gqueries):

        # convert to series        
        if not isinstance(gqueries, pd.Series):
            gqueries = pd.Series(gqueries)
        
        # set gqueries
        self.__gqueries = gqueries

    @property
    def depricated(self) -> list[str]:

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

    def __init__(self, session_ids: pd.Series,
        parameters: pd.Series, gqueries: pd.Series, **kwargs):
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

        # set kwargs
        self._kwargs = kwargs

    @classmethod
    def from_excel(cls, filepath: str, **kwargs):
        """initate from excel file with standard structure"""

        # get session ids
        session_ids = pd.read_excel(filepath, sheet_name='Sessions', 
            usecols=[*range(5)], index_col=[*range(4)]).squeeze('columns')

        # get paramters
        parameters = pd.read_excel(filepath, sheet_name='Parameters', 
            usecols=[*range(2)], index_col=[*range(1)]).squeeze('columns')

        # get gqueries
        gqueries = pd.read_excel(filepath, sheet_name='GQueries', 
            usecols=[*range(2)], index_col=[*range(1)]).squeeze('columns')

        return cls(session_ids, parameters, gqueries, **kwargs)

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
        
        # warn for unmapped keys
        if not missing.empty:
            _logger.warn("'%s' returned unmapped parameters: %s",
                client, list(missing))

        return missing

    def get_input_parameters(self, 
            midx: tuple | pd.MultiIndex | None = None) -> pd.DataFrame:
        """get input parameters for single scenario"""

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

        _logger.info("collecting input parameters")

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                values = []

                # connect scenario and check for unmapped keys
                client.gqueries = list(self.gqueries.index)

                # get parameter settings
                for case, scenario_id in cases.iteritems():

                    # log event
                    _logger.debug("> collecting inputs for " + 
                        "'%s', '%s', '%s', '%s'", *case)

                    # connect scenario and check for unmapped keys
                    client.scenario_id = scenario_id
                    self._check_for_unmapped_input_parameters(client)

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

        except Exception as error:

            # make filepath
            now = datetime.datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + '.log')

            # log excteption as error
            _logger.error("Encountered error: exported logs to '%s'", filepath)
            _logger.debug("Traceback for encountered error:", exc_info=True)

            # export logfile
            export_logfile(filepath)

            raise error

        # construct frame and handle nulls
        frame = pd.concat(values, axis=1, keys=midx)
        frame = frame.fillna(np.nan)

        # set mapped units as index level
        units = frame.index.map(self.parameters)
        frame = frame.set_index(units, append=True)

        # set names of index levels
        frame.index.names = ['KEY', 'UNIT']
        frame.columns.names = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']

        return sort_frame(frame)

    def set_input_parameters(self, 
        frame: pd.DataFrame) -> None:
        """set input parameters"""

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
            raise KeyError("unknown cases in dataframe: '%s'" %errors)

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
                _logger.warning("excluded '%s' from upload" %key)

            # drop excluded parameters
            frame = frame.drop(illegal)

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # iterate over cases
                for case, values in frame.iteritems():

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

        except Exception as error:

            # make filepath
            now = datetime.datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + '.log')

            # log excteption as error
            _logger.error("Encountered error: exported logs to '%s'", filepath)
            _logger.debug("Traceback for encountered error:", exc_info=True)

            # export logfile
            export_logfile(filepath)

            raise error

    def get_output_values(self, 
            midx: tuple | pd.MultiIndex | None = None) -> pd.DataFrame:
        """get input parameters for single scenario"""

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

        _logger.info("collecting gquery results")

        try:

            # make client with context manager
            with Client(**self._kwargs) as client:

                # newlist
                values = []

                # connect scenario and check for unmapped keys
                client.gqueries = list(self.gqueries.index)

                # get parameter settings
                for case, scenario_id in cases.iteritems():

                    # log event
                    _logger.debug("> collecting gquery results for " + 
                        "'%s', '%s', '%s', '%s'", *case)

                    # connect scenario and set gqueries
                    client.scenario_id = scenario_id
                    gqueries = client.gquery_results['future']

                    # append gquery results
                    values.append(gqueries)

        # log and raise exception
        except Exception as error:

            # make filepath
            now = datetime.datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + '.log')

            # log excteption as error
            _logger.error("Encountered error: exported logs to '%s'", filepath)
            _logger.debug("Traceback for encountered error:", exc_info=True)

            # export logfile
            export_logfile(filepath)

            raise error

        # construct frame and handle nulls
        frame = pd.concat(values, axis=1, keys=midx)
        frame = frame.fillna(np.nan)

        # set mapped units as index level
        units = frame.index.map(self.gqueries)
        frame = frame.set_index(units, append=True)

        # set names of index levels
        frame.index.names = ['KEY', 'UNIT']
        frame.columns.names = ['STUDY', 'SCENARIO', 'REGION', 'YEAR']

        # aggregate eu27
        frame = frame.fillna(0)

        return sort_frame(frame)

    def make_myc_urls(self, 
            midx: tuple | pd.MultiIndex | None = None) -> pd.Series:
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

        # drop reference scenario
        scenarios = cases.index.get_level_values(level='SCENARIO')
        if 'Reference' in scenarios.unique():
            cases = cases.drop('Reference', level='SCENARIO', axis=0)

        _logger.info("making MYC URLS")

        # unstack year
        scenarios = cases.unstack(level='YEAR').astype(str)

        # raise for missing ids 
        if scenarios.isna().values.any():
            raise ValueError("missing session ids")

        # convert paths to MYC urls
        urls = "https://myc.energytransitionmodel.com/"
        urls += scenarios.apply(lambda x: ",".join(x), axis=1) + '/inputs'

        return pd.Series(urls, name='URL').sort_index()

    def to_excel(self, 
        filepath: str | None = None,
        midx: tuple | pd.MultiIndex | None = None, 
        input_parameters: pd.DataFrame | None = None, 
        output_values: pd.DataFrame | None = None, 
        myc_urls: pd.Series | None = None) -> None:
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
        urls : pd.Series, default None
            Series to write on the url sheet
            of the Excel. Default to make_myc_urls
            method with specified midx."""

        from pathlib import Path
        from .utils.excel import add_frame, add_series

        if TYPE_CHECKING:
            # import xlswriter
            import xlsxwriter

        else:
            # import optional dependency
            xlsxwriter = import_optional_dependency('xlsxwriter')

        # make filepath
        if filepath is None:
            
            # default filepath
            now = datetime.datetime.now().strftime("%Y%m%d%H%M")
            filepath = Path.cwd().joinpath(now + '.xlsx')

        # check filepath
        if not Path(filepath).parent.exists:
            raise FileNotFoundError("Path to file does not exist: '%s'" 
                %filepath)

        # create workbook
        workbook = xlsxwriter.Workbook(str(filepath))

        # default inputs
        if input_parameters is None:
            input_parameters = self.get_input_parameters(midx=midx)

        # add inputs to workbook
        add_frame('INPUT_PARAMETERS', input_parameters, workbook)

        # default outputs
        if output_values is None:
            output_values = self.get_output_values(midx=midx)

        # add outputs to workbook
        add_frame('OUTPUT_VALUES', output_values, workbook)

        # default urls
        if myc_urls is None:
            myc_urls = self.make_myc_urls(midx=midx)

        # add urls to workbook
        worksheet = add_series('ETM_URLS', myc_urls, workbook)

        # set column widths
        worksheet.set_column(0, 2, 20)
        worksheet.set_column(3, 3, 80)

        # write workbook
        workbook.close()

        _logger.info("exported results to '%s'", filepath)
