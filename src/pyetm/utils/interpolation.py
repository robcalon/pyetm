"""scenario interpolation"""

from __future__ import annotations
from typing import Iterable, TYPE_CHECKING

# import os
import logging

import pandas as pd
from pyetm.types import ErrorHandling, InterpolateOptions

# from pyetm import Client

if TYPE_CHECKING:
    from pyetm import Client

logger = logging.getLogger(__name__)


# def interpolate_saved_scenario_ids(
#     target: int | Iterable[int],
#     scenario_ids: Iterable[int],
#     method: InterpolateOptions = "linear",
#     if_errors: ErrorHandling = "raise",
#     **kwargs
# ) -> pd.DataFrame:
#     """copy saved scenario ids and delete after use

#     # consider read_only access to saved_scenario_ids
#     # (to ensure history is consistent)
#     """

#     # ensure token
#     if 'token' not in kwargs.keys():
#         if os.getenv('ETM_ACCESS_TOKEN') is None:
#             raise ValueError("must pass token")

#     # create clients
#     clients = [
#         Client.from_saved_scenario_id(sid, **kwargs)
#         for sid in scenario_ids
#     ]

#     # interpolate scenarios
#     interpolated = interpolate(
#         target=target,
#         clients=clients,
#         method=method,
#         if_errors=if_errors
#     )

#     # clean up scenarios
#     for client in clients:
#         client.delete_scenario()

#     return interpolated

def interpolate(
    target: int | Iterable[int],
    # clients: Iterable[int] | Iterable[Client],
    clients: Iterable[Client],
    method: InterpolateOptions = "linear",
    if_errors: ErrorHandling = "raise",
    # **kwargs
) -> pd.DataFrame:
    """Interpolates the user values of the years between the
    passed clients. Uses a seperate method for continous and
    discrete user values.

    Do note that the heat network order is not returned or
    interpolated by this function.

    Parameters
    ----------
    target : int or iterable of int
        The target year(s) for which to
        make an interpolation.
    clients: list of Client
        List of pyetm.client.Client objects that
        are used to interpolate the scenario.
    method : string, default 'linear'
        Method for filling continious user values
        for the passed target year(s).

    Returns
    -------
    inputs : DataFrame
        Returns the input parameters for all years of the
        passed clients and the target year(s)."""

    # _clients: list[Client] = []
    # for client in clients:
    #     if not isinstance(client, Client):
    #         _clients.append(
    #             Client(scenario_id=client, **kwargs)
    #         )

    # sort clients by end year
    _clients = sorted(clients, key=lambda client: client.end_year)

    # handle single target year
    if isinstance(target, int):
        target = [target]

    # validate area codes for clients
    codes = [cln.area_code for cln in _clients]
    if len(set(codes)) != 1:
        raise ValueError(f"Different area codes in passed clients: {codes}")

    # validate end years
    years = [cln.end_year for cln in _clients]
    if len(set(years)) != len(_clients):
        raise ValueError(f"Duplicate end years in passed clients: {years}")

    # filter list
    filtered = [yr for yr in target if min(years) < yr < max(years)]
    if len(set(filtered)) != len(set(target)):
        raise ValueError(
            "Interpolation target(s) out of bound: "
            f"{min(years)} < "
            f"{list(set(filtered).symmetric_difference(target))} "
            f"< {max(years)}."
        )

    # merge inputs and mask get input parameters
    inputs = pd.concat([cln.input_parameters for cln in _clients], axis=1, keys=years)
    params = _clients[0].get_input_parameters(include_disabled=False, detailed=True)

    # split input parameters by value type
    mask = params["unit"].isin(["enum", "x", "bool"])
    cinputs, dinputs = inputs.loc[~mask], inputs.loc[mask]

    # check for equality of discrete values
    errors = dinputs.loc[~dinputs.eq(dinputs.iloc[:, -1], axis=0).any(axis=1)]
    if not errors.empty:
        # make message
        msg = (
            "Inconsistent scenario settings for input parameters: \n\n"
            + errors.to_string()
        )

        if if_errors == "warn":
            logger.warning(msg)

        if if_errors == "raise":
            raise ValueError(msg)

    # expand subsets with target year columns
    columns = sorted(set(years).union(filtered))
    cinputs = pd.DataFrame(data=cinputs, columns=columns, dtype=float)
    dinputs = pd.DataFrame(data=dinputs, columns=columns)

    # handle interpolation
    cinputs = cinputs.interpolate(method=method, axis=1)
    dinputs = dinputs.bfill(axis=1)

    return pd.concat([cinputs, dinputs])
