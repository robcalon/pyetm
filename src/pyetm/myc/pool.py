"""ClientPool and concurrent tasks"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import Queue
from typing import get_args, Callable, Generator, Hashable, Iterable
from traceback import format_exception_only

import logging
import pandas as pd

from pyetm import Client
from pyetm.types import Carrier
from pyetm.utils.categorisation import assigin_sign_convention
from pyetm.utils.general import iterable_to_str

logger = logging.getLogger(__name__)
Scenarios = dict[Hashable, int] | pd.Series
ListOfStrLike = Iterable[str] | pd.Series

def validate_carrier(carrier: Carrier) -> Carrier:
    """validate if carrier is supported"""
    if carrier not in get_args(Carrier):
        raise ValueError(f"Unsupported carrier: {carrier}")
    return carrier

def validate_carrier_sequence(carriers: Carrier | Iterable[Carrier]) -> list[Carrier]:
    """validate if carrier sequence contains supported carrier"""

    # handle single carrier
    if isinstance(carriers, str):
        carriers = [carriers]

    # subset errors
    errors = set(car for car in carriers if car not in get_args(Carrier))
    if errors:
        raise ValueError(f"Unsupported carriers in sequence: {iterable_to_str(errors)}")

    return list(carriers)

class PoolTasks:
    """Pool Tasks"""

    @staticmethod
    def get_parameters(
        pool: ClientPool,
        scenario_id: int,
        parameters: ListOfStrLike | None = None,
        exclude: bool = False
    ) -> pd.Series:
        """return inputs"""

        # TODO: Would be nice to split in parameters and settings.
        # See related github issue on ETEngine

        # get inputs from scenario endpoint
        with pool.get_client_from_session_id(scenario_id) as client:
            inputs = client.get_input_parameters(detailed=True, include_disabled=True)

        # find unused coupling nodes
        mask = ~inputs['disabled'] & inputs['coupling_groups']
        uncoupled = [key for group in inputs.loc[mask]['disabled_by'] for key in group]

        # TODO: warn for incompatible authorisation (read only) scope
        # This case results in all parameters having a disabled is true property
        # See related github issue on ETEninge

        # drop disabled and inactive parameters
        inputs = inputs[~inputs['disabled']]
        inputs = inputs.drop(index=uncoupled)

        # filter parameters
        if parameters is not None:
            mask = inputs.index.isin(parameters)
            inputs = inputs.loc[~mask] if exclude else inputs.loc[mask]

        # add unit to index
        inputs['unit'] = inputs['unit'].replace({'x': 'bool', 'enum': 'literal'})
        inputs = inputs.set_index('unit', append=True)

        # rename indeces and fill user settings
        inputs.index = inputs.index.set_names(names=['parameter', 'unit'])
        inputs['user'] = inputs['user'].fillna(inputs['default'])

        # use booleans
        mask = inputs.index.get_level_values(level='unit').isin({'x', 'bool'})
        inputs.loc[mask, 'user'] = inputs.loc[mask, 'user'].astype(bool) # pyright: ignore

        return inputs['user'].rename(scenario_id)

    @staticmethod
    def set_parameters(
        pool: ClientPool,
        scenario_id: int,
        parameters: pd.Series | pd.DataFrame,
    ) -> pd.Series:
        """set parameters"""

        # subset parameters
        if isinstance(parameters, pd.DataFrame):
            parameters = parameters.loc[:, scenario_id]

        # TODO: dropna in client function
        # set parameters
        with pool.get_client_from_session_id(scenario_id) as client:
            client.set_input_parameters(parameters)

        return pd.Series(name=scenario_id)

    @staticmethod
    def get_gqueries(
        pool: ClientPool,
        scenario_id: int,
        gqueries: ListOfStrLike,
    ) -> pd.Series:
        """return gqueries"""

        # get gquery result
        with pool.get_client_from_session_id(scenario_id) as client:
            client.gqueries = gqueries
            _gqueries = client.get_gquery_results()

        # reformat results
        _gqueries = _gqueries.set_index('unit', append=True)
        _gqueries.index = _gqueries.index.rename(['gquery', 'unit'])

        return _gqueries['future'].rename(scenario_id)

    @staticmethod
    def get_price_curves(
        pool: ClientPool,
        scenario_id: int,
        carriers: Carrier | Iterable[Carrier] | None = None,
    ) -> pd.Series:
        """return hourly price curve"""

        # default carrier
        if carriers is None:
            carriers = get_args(Carrier)

        carriers = validate_carrier_sequence(carriers)

        # only electricity
        for carrier in carriers:
            if carrier != 'electricity':
                logger.debug("Excluded export of hourly %s price curves (NotImplemented in ETM).")
        carriers = ['electricity']

        curves = []
        for carrier in carriers:

            # TODO: Replace with client.get_price_curve(carrier=carrier)
            # Required update in pyETM
            # Don't forget to remove defaulting to electricity here.

            # get price curve
            attr = f"get_hourly_{carrier}_price_curve"
            with pool.get_client_from_session_id(scenario_id) as client:
                curve = getattr(client, attr)()

            # reformat index
            curve = curve.reset_index(drop=True)
            curves.append(curve.rename(carrier))

        # concat carrier price curves
        curves = pd.concat(curves, axis=1)

        # stack carriers
        curves = curves.T.stack()
        curves.index = curves.index.rename(['carrier', 'hour'])

        # validate outcome
        if not isinstance(curves, pd.Series):
            raise TypeError(f"Expected Series instead of {type(curves)}.")

        return curves.rename(scenario_id)

    @staticmethod
    def get_carrier_curves(
        pool: ClientPool,
        scenario_id: int,
        carrier: Carrier,
        invert_sign_convention: bool = False
    ) -> pd.Series:
        """return hourly carrier curve"""

        carrier = validate_carrier(carrier)

        # TODO: Replace with client.get_carrier_curves(carrier=carrier)
        # Requires update in pyETM.

        attr = f"get_hourly_{carrier}_curves"
        with pool.get_client_from_session_id(scenario_id) as client:
            curves: pd.DataFrame = getattr(client, attr)()

        # reset index and assign sign convention
        curves = curves.reset_index(drop=True)
        curves = assigin_sign_convention(curves, invert_sign=invert_sign_convention)

        # invert curve signs
        if invert_sign_convention is True:
            raise NotImplementedError("Implementation pending")

        # stack curve keys
        series = curves.T.stack()
        series.index = series.index.rename(['curve', 'hour'])

        if not isinstance(series, pd.Series):
            raise TypeError("returned unexpected instance type")

        # prepend carrier to index levels
        series = pd.concat([series], keys=[carrier], names=['carrier'])

        return series.rename(scenario_id)

    # @staticmethod
    # def get_climate_years(
    #     pool: ClientPool,
    #     scenario_id: int,
    #     carrier: Carrier,
    #     ccurves: pd.DataFrame,
    #     invert_sign: bool = False,
    # ) -> pd.DataFrame:

    #     carrier = validate_carrier(carrier)

    #     attr = f"get_hourly_{carrier}_curves"
    #     with pool.get_client_from_session_id(scenario_id) as client:
    #         pass

    @staticmethod
    def upload_custom_curves(
        pool: ClientPool,
        scenario_id: int,
        ccurves: pd.Series | pd.DataFrame,
    ) -> pd.Series:
        """upload custom curves"""

        with pool.get_client_from_session_id(scenario_id) as client:
            client.upload_custom_curves(ccurves=ccurves)

        return pd.Series(name=scenario_id)

    @staticmethod
    def delete_custom_curves(
        pool: ClientPool,
        scenario_id: int,
        keys: str | Iterable[str] | None = None,
    ) -> pd.Series:
        """delete custom curves"""

        with pool.get_client_from_session_id(scenario_id) as client:
            client.delete_custom_curves(keys=keys)

        return pd.Series(name=scenario_id)

    @staticmethod
    def set_custom_curves(
        pool: ClientPool,
        scenario_id: int,
        ccurves: pd.Series | pd.DataFrame,
    ) -> pd.Series:
        """set custom curves"""

        with pool.get_client_from_session_id(scenario_id) as client:
            client.set_custom_curves(ccurves=ccurves)

        return pd.Series(name=scenario_id)



class ClientPool:
    """pool of reusable clients"""

    def __init__(
        self,
        maxsize: int | None = None,
        clients: list[Client] | None = None,
        **kwargs
    ) -> None:

        if maxsize is None:
            maxsize = len(clients) if clients else 3

        self.maxsize = maxsize
        self.clients = clients

        if clients is None:
            clients = [Client(**kwargs) for _ in range(maxsize)]

        self._pool: Queue[Client] = Queue(maxsize=maxsize)

        for idx in range(maxsize):
            self._pool.put(clients[idx])

        self.tasks = PoolTasks()

    @contextmanager
    def get_client_from_session_id(
        self,
        scenario_id: int,
        ccurves: pd.DataFrame | Callable | None = None,
        from_saved_scenario_id: bool = False, # will become default approach
        # update_scenario: bool = False, # save result to saved scenario id
        **kwargs
    ) -> Generator[Client, None, None]:
        """get client from pool"""

        client = self._pool.get()

        # TODO: Except copy scenario to return client instead of int
        if from_saved_scenario_id is True:
            scenario_id  = client._get_saved_scenario_id(scenario_id)
            client.copy_scenario(scenario_id)

        else:
            client.scenario_id = scenario_id

        # TODO: collect profiles with callable
        if isinstance(ccurves, Callable):
            ccurves = ccurves(**kwargs)

        if ccurves is not None:

            if not isinstance(ccurves, (pd.Series, pd.DataFrame)):
                raise TypeError(f"excepted dataframe instead of {type(ccurves)}")

            # TODO: Update uplaod custom curves (use filename instead of filenames)
            filename = kwargs.get('filename')
            if filename is None:
                filename = kwargs.get('filenames')

            client.upload_custom_curves(ccurves, filenames=filename)

        try:
            yield client

        finally:
            if from_saved_scenario_id is True:
                sid = client.scenario_id

                try:
                    client.delete_scenario()

                except Exception as exc:
                    msg = ''.join(format_exception_only(type(exc), exc)).rstrip()
                    logger.debug(
                        "Deletion of scenario %s (copy from: %s) failed due to error: '%s'",
                        sid, scenario_id, msg
                    )

            self._pool.put(client)

    def call_threaded(
        self,
        func: Callable,
        scenarios: Scenarios,
        wait: bool = False,
        cancel_futures: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """helper function to collect callables with threadpool"""

        results = {}
        with ThreadPoolExecutor(max_workers=self._pool.maxsize) as executor:
            futures = {
                scenario: executor.submit(
                    func, pool=self, scenario_id=sid, **kwargs
                ) for scenario, sid in scenarios.items()
            }

            # sequential handle of completed futures
            # Reminder: as_completed does not work when futures are cancelled
            for scenario, future in futures.items():

                # handle exceptions
                exc = future.exception()
                if exc:

                    # report dropped case and log exception
                    msg = ''.join(format_exception_only(type(exc), exc)).rstrip()
                    logger.warning("Excluded scenario '%s' from results due to error: '%s'", scenario, msg)
                    logger.debug("Traceback for scenario '%s':", scenario, exc_info=exc)

                    # handle executor
                    executor.shutdown(wait=wait, cancel_futures=cancel_futures)

                # handle evaluated cases
                elif not future.cancelled():
                    results[scenario] = future.result()

        if not results:
            return pd.DataFrame()

        if not isinstance(scenarios, pd.Series):
            scenarios = pd.Series(scenarios)

        return pd.concat(results, axis=1, names=scenarios.index.names)

    def get_parameters(
        self,
        scenarios: Scenarios,
        parameters: ListOfStrLike | None = None,
        exclude: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """get parameters"""
        return self.call_threaded(
            func=self.tasks.get_parameters,
            scenarios=scenarios,
            parameters=parameters,
            exclude=exclude,
            **kwargs
        )

    def set_parameters(
        self,
        scenarios: Scenarios,
        parameters: pd.DataFrame,
        **kwargs
    ) -> None:
        """set parameters"""

        # use series object
        if not isinstance(scenarios, pd.Series):
            scenarios = pd.Series(scenarios)

        # assess if mappable
        mask1 = scenarios.isin(parameters.columns)
        mask2 = scenarios.index.isin(parameters.columns)

        # raise unmappable
        if not (all(mask1) or all(mask2)):
            raise KeyError("Could not find all scenarios in header")

        # map to scenario ids
        if all(mask2):
            parameters.columns = parameters.columns.map(scenarios)

        self.call_threaded(
            func=self.tasks.set_parameters,
            scenarios=scenarios,
            parameters=parameters,
            **kwargs
        )

    def get_gqueries(
        self,
        scenarios: Scenarios,
        gqueries: ListOfStrLike,
        **kwargs
    ) -> pd.DataFrame:
        """get gqueries"""

        if gqueries is None:
            raise ValueError("No gqueries specified")

        return self.call_threaded(
            func=self.tasks.get_gqueries,
            scenarios=scenarios,
            gqueries=gqueries,
            **kwargs
        )

    def get_price_curves(
        self,
        scenarios: Scenarios,
        carriers: Carrier | Iterable[Carrier] | None = None,
        **kwargs
    ) -> pd.DataFrame:
        """get hourly price curves"""
        return self.call_threaded(
            func=self.tasks.get_price_curves,
            scenarios=scenarios,
            carriers=carriers,
            **kwargs
        )

    # TODO: Update to support multiple carriers?
    # see example in weathertools
    def get_carrier_curves(
        self,
        scenarios: Scenarios,
        carrier: Carrier,
        invert_sign_convention: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """get hourly carrier curves"""
        return self.call_threaded(
            func=self.tasks.get_carrier_curves,
            scenarios=scenarios,
            carrier=carrier,
            invert_sign_convention=invert_sign_convention,
            **kwargs,
        )

    def upload_custom_curves(
        self,
        scenarios: Scenarios,
        ccurves: pd.Series | pd.DataFrame,
        **kwargs
    ) -> None:
        """upload custom curves"""
        self.call_threaded(
            func=self.tasks.upload_custom_curves,
            scenarios=scenarios,
            curves=ccurves,
            **kwargs,
        )

    def delete_custom_curves(
        self,
        scenarios: Scenarios,
        keys: str | Iterable[str] | None = None,
        **kwargs
    ) -> None:
        """delete custom curves"""
        self.call_threaded(
            func=self.tasks.delete_custom_curves,
            scenarios=scenarios,
            keys=keys,
            **kwargs,
        )

    def set_custom_curves(
        self,
        scenarios: Scenarios,
        ccurves: pd.Series | pd.DataFrame,
        **kwargs
    ) -> None:
        """set custom curves"""
        self.call_threaded(
            func=self.tasks.set_custom_curves,
            scenarios=scenarios,
            curves=ccurves,
            **kwargs,
        )
