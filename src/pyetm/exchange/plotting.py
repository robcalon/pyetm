"""plotting methods"""
from __future__ import annotations

import math

from pathlib import Path
from datetime import datetime
from dateutil import parser

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from matplotlib.axes import Axes

def _read_items(hour: int, items: list[str], wdir: str | Path | None = None):
    """read items"""

    # default wdir
    if wdir is None:
        wdir = Path.cwd()

    def read_curve(item):
        """read indivual curve"""

        filename = Path(wdir).joinpath(f'{item}.csv')
        curve = pd.read_csv(filename, sep=';', decimal=',',
                            header=None, usecols=[hour]).squeeze('columns')

        return pd.Series(curve, name=item, dtype='float64')

    return pd.concat([read_curve(item) for item in items], axis=1)

def plot_price_convergence(hour: int, regions: list[str],
    axs: Axes | None = None, wdir: str | Path | None = None, **kwargs):
    """plot price convergence over regions"""

    if wdir is None:
        wdir = Path.cwd()

    filedir = Path(wdir).joinpath('prices')

    # specify subplot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # read data
    data = _read_items(hour, regions, filedir)
    data.plot(axs=axs, linewidth=3)

    # set legend
    ncol = math.ceil(len(regions) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # axis limits
    axs.set_xlim(0, len(data) - 1)
    axs.set_ylim(0, None)

    # set axis labels
    axs.set_title('Price convergence', size=12)
    axs.set_ylabel('Price', size=10)
    axs.set_xlabel('Iteration [#]', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs

def plot_utilization_traces(hour: int, interconnectors: list[str],
    axs: Axes | None = None, wdir: str | Path | None = None, **kwargs):
    """plot interconnector utilization traces"""

    if wdir is None:
        wdir = Path.cwd()

    filedir = Path(wdir).joinpath('utilization')

    # specify subplot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # read data
    data = _read_items(hour, interconnectors, filedir)
    data.plot(axs=axs, linewidth=3)

    # set legend
    ncol = math.ceil(len(interconnectors) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # axis limits
    axs.set_xlim(0, len(data) - 1)
    axs.set_ylim(-1.1, 1.1)

    # set axis labels
    axs.set_title('Interconnector utilization', size=12)
    axs.set_ylabel('Utilization / Capacity', size=10)
    axs.set_xlabel('Iteration [#]', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs

def plot_prediction_error(hour: int, interconnectors: list[str],
    axs: Axes | None = None, wdir: str | Path | None = None, **kwargs):
    """plot model prediction error traces"""

    if wdir is None:
        wdir = Path.cwd()

    filedir = Path(wdir).joinpath('difference')

    # specify subplot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # read data
    data = _read_items(hour, interconnectors, filedir)
    data.plot(axs=axs, linewidth=3)

    # set legend
    ncol = math.ceil(len(interconnectors) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # axis limits
    axs.set_xlim(0, len(data) - 1)
    axs.set_ylim(-1.1, 1.1)

    # set axis labels
    axs.set_title('Prediction error', size=12)
    axs.set_ylabel('Model minus ETM', size=10)
    axs.set_xlabel('Iteration [#]', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs

def plot_symmetry_error(hour: int, interconnectors: list[str],
    axs: Axes | None = None, wdir: str | Path | None = None, **kwargs):
    """plot model symmetry error traces"""

    if wdir is None:
        wdir = Path.cwd()

    filedir = Path(wdir).joinpath('consistency')

    # specify subplot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # read data
    data = _read_items(hour, interconnectors, filedir)
    data.plot(axs=axs, linewidth=3)

    # set legend
    ncol = math.ceil(len(interconnectors) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # axis limits
    axs.set_xlim(0, len(data) - 1)
    axs.set_ylim(-1.1, 1.1)

    # set axis labels
    axs.set_title('Symmetry error', size=12)
    axs.set_ylabel('From minus to', size=10)
    axs.set_xlabel('Iteration [#]', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs

def plot_monitor(regions: list[str], interconnectors: list[str], hour: int,
    wdir: str | Path | None = None):
    """plot trace monitioring for hour"""

    if wdir is None:
        wdir = Path.cwd()

    # make figure
    _, axs = plt.subplots(4, 1, figsize=(15, 10), sharex=True)

    # plot monitored metrics
    plot_price_convergence(hour, regions, axs=axs[0], wdir=wdir)
    plot_utilization_traces(hour, interconnectors, axs=axs[1], wdir=wdir)
    plot_prediction_error(hour, interconnectors, axs=axs[2], wdir=wdir)
    plot_symmetry_error(hour, interconnectors, axs=axs[3], wdir=wdir)

    return _, axs

def exchange_volumes_for_region(region, model, utilization=None):
    """get exchange volumes for all connector regions of the
    specified region"""

    # subset region relevant interconnectors
    imap = model._interconnector_mapping
    conns = imap.xs(region, level=0).set_index('key')

    # default to model utilization
    if utilization is None:
        utilization = model.interconnector_utilization

    # subset relevant interconnectors
    utilization = utilization[conns.index]
    capacity = model.interconnector_capacity[conns.index]

    # scale and orient utilization
    orient = np.where(conns['from_region'] == region, 1, -1)
    utilization *= capacity * orient

    # rename columns and sort
    utilization.columns = utilization.columns.map(conns['other'])
    utilization = utilization.sort_index(axis=1)

    return utilization

def plot_weekly_exchange(region, model, utilization=None, axs=None, **kwargs):
    """plot weekly exchange volumes on week basis for a region"""

    # get exchange volumes for region
    utilization = exchange_volumes_for_region(region, model, utilization)
    utilization = utilization.loc[:8735]

    # default plot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # configuration
    periods = 8736
    origin = parser.parse(
        '01-01-2029 00:00', dayfirst=True)

    # make periodindex
    utilization.index = pd.period_range(start=origin,
        periods=periods, freq='H', name='DateTime')

    # # aggregate utilization on weekly basis
    grouper = pd.Grouper(freq='W')
    utilization = utilization.groupby(grouper).sum() / 1e6

    # plot utilization
    utilization.index = utilization.index.week
    utilization.plot.bar(axs=axs, stacked=True)

    # set legend
    ncol = math.ceil(len(utilization.columns) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # set axis labels
    axs.set_title(f'Weekly exchange volumes {region}', size=12)
    axs.set_ylabel('Volume [TWh]', size=10)
    axs.set_xlabel('Week', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)
    # axs.xaxis.set_major_formatter(mdates.DateFormatter("%m"))

    return axs

def plot_daily_exchange(region, model, utilization=None,
    from_date=None, to_date=None, axs=None, **kwargs):
    """plot weekly exchange volumes on week basis for a region"""

    # get exchange volumes for region
    utilization = exchange_volumes_for_region(region, model, utilization)

    # default from date
    if from_date is None:
        from_date = parser.parse(
            '01-01-2029 00:00', dayfirst=True)

    # default to date
    if to_date is None:
        to_date = parser.parse(
            '01-03-2029 23:00', dayfirst=True)

    # default plot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # configuration
    periods = 8760
    origin = parser.parse(
        '01-01-2029 00:00', dayfirst=True)

    # make periodindex
    utilization.index = pd.period_range(start=origin,
        periods=periods, freq='H', name='DateTime')

    # convert from date from string
    if not isinstance(from_date, datetime):
        from_date = parser.parse(from_date, dayfirst=True).replace(
                year=origin.year, hour=0, minute=0, second=0, microsecond=0)

    # convert to date from string
    if not isinstance(to_date, datetime):
        to_date = parser.parse(to_date, dayfirst=True).replace(
            year=origin.year, hour=23, minute=0, second=0, microsecond=0)

    # subet period
    utilization = utilization.loc[from_date:to_date]

    # # aggregate utilization on weekly basis
    grouper = pd.Grouper(freq='D')
    utilization = utilization.groupby(grouper).sum() / 1e3

    # plot utilization
    utilization.index = utilization.index.strftime('%b %d')
    utilization.plot.bar(axs=axs, stacked=True)

    # set legend
    ncol = math.ceil(len(utilization.columns) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # set axis labels
    axs.set_title(f'Daily exchange volumes {region}', size=12)
    axs.set_ylabel('Volume [GWh]', size=10)
    axs.set_xlabel('Date', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs

def plot_hourly_exchange(region, model, utilization=None,
    from_date=None, to_date=None, axs=None, **kwargs):
    """plot weekly exchange volumes on week basis for a region"""

    # get exchange volumes for region
    utilization = exchange_volumes_for_region(region, model, utilization)

    # default from date
    if from_date is None:
        from_date = parser.parse(
            '01-01-2029 00:00', dayfirst=True)

    # default to date
    if to_date is None:
        to_date = parser.parse(
            '03-01-2029 23:00', dayfirst=True)

    # default plot
    if axs is None:
        _, axs = plt.subplots(**kwargs)

    # configuration
    periods = 8760
    origin = parser.parse(
        '01-01-2029 00:00', dayfirst=True)

    # make periodindex
    utilization.index = pd.period_range(start=origin,
        periods=periods, freq='H', name='DateTime')

    # convert from date from string
    if not isinstance(from_date, datetime):
        from_date = parser.parse(from_date, dayfirst=True).replace(
                year=origin.year, minute=0, second=0, microsecond=0)

    # convert to date from string
    if not isinstance(to_date, datetime):
        to_date = parser.parse(to_date, dayfirst=True).replace(
            year=origin.year, minute=0, second=0, microsecond=0)

    # subet period
    utilization = utilization.loc[from_date:to_date]

    # plot utilization
    utilization.index = utilization.index.strftime('%e %b - %H:%M')
    utilization.plot.bar(axs=axs, stacked=True)

    # set legend
    ncol = math.ceil(len(utilization.columns) / 5)
    axs.legend(loc='center left', title='Legend', ncol=ncol,
        fontsize=11, bbox_to_anchor=(1.01, 0.5))

    # set axis labels
    axs.set_title(f'Hourly exchange volumes {region}', size=12)
    axs.set_ylabel('Volume [MWh]', size=10)
    axs.set_xlabel('Date', size=10)

    # set ticks
    axs.tick_params(axis='both', which='major', labelsize=10)

    return axs
