"""Generate weather profiles based on climate datasets"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from os import PathLike
from pathlib import Path
from traceback import format_exception_only

import functools

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.types import WeatherProfileMapping, RegionMapping, StrOrPath
from pyetm.utils.validators import validate_capacity_factors

from .io import ParquetHandler
from .io.abc import IOHandler
from .models import HeatDemandProfileGenerator
from .pecd import PECD31CSVReader, PECD41CSVReader
from .pecd.abc import PECDReader
from .scalers.abc import ProfileScaler

logger = get_modulelogger(__name__)

class WeatherProfileGenerator:
    """Profile Generator for ETM uploads"""

    # Unit conversion factors
    _irradiance_ucf = 1
    _temperature_ucf = 1
    _windspeed_ucf = 1

    @property
    def reader(self) -> PECDReader:
        """PECD Reader"""
        return self._reader

    @reader.setter
    def reader(self, reader: PECDReader) -> None:

        # ensure inheritance
        if not isinstance(reader, PECDReader):
            raise TypeError(
                f"reader must inherit from PECDReader class: {type(reader)}"
            )

        self._reader = reader

    @property
    def scaler(self) -> ProfileScaler:
        """Profile scaler"""

        if self._scaler is None:
            raise AttributeError("Scaler not assigned.")

        return self._scaler

    @scaler.setter
    def scaler(self, scaler: ProfileScaler | None) -> None:

        # ensure inheritance
        if scaler is not None:
            if not isinstance(scaler, ProfileScaler):
                raise TypeError(
                    f"scaler must inherit from ProfileScaler: {type(scaler)}"
                )

        self._scaler = scaler

    @property
    def heat_demand_generator(self) -> HeatDemandProfileGenerator:
        """Heat demand generator"""
        return self._heat_demand_generator

    @heat_demand_generator.setter
    def heat_demand_generator(self, generator: HeatDemandProfileGenerator | None) -> None:

        # default generator
        if generator is None:
            generator = HeatDemandProfileGenerator.from_defaults()

        self._heat_demand_generator = generator

    @property
    def writer(self) -> IOHandler:
        """Profile writer"""
        return self._writer

    @property
    def ucf_irradiance(self) -> float:
        """Irradiance unit conversion factor"""
        return self._irradiance_ucf

    @ucf_irradiance.setter
    def ucf_irradiance(self, ucf: float | None):

        ucf = ucf if ucf else 1
        if self._irradiance_ucf != ucf:
            self._irradiance_ucf = ucf
            logger.info("Irradiance unit conversion factor configured: ucf=%f", ucf)

    @property
    def ucf_temperature(self) -> float:
        """Temperature unit conversion factor"""
        return self._temperature_ucf

    @ucf_temperature.setter
    def ucf_temperature(self, ucf: float | None):

        ucf = ucf if ucf else 1
        if self._temperature_ucf != ucf:
            self._temperature_ucf = ucf
            logger.info("Temperature unit conversion factor configured: ucf=%f", ucf)

    @property
    def ucf_windspeed(self) -> float:
        """Windspeed unit conversion factor"""
        return self._windspeed_ucf

    @ucf_windspeed.setter
    def ucf_windspeed(self, ucf: float | None):

        ucf = ucf if ucf else 1
        if self._windspeed_ucf != ucf:
            self._windspeed_ucf = ucf
            logger.info("Windspeed unit conversion factor configured: ucf=%f", ucf)

    @writer.setter
    def writer(self, writer: IOHandler | StrOrPath | None) -> None:

        # make default writer path
        if writer is None:

            # default wdir
            writer = Path.cwd().joinpath('weather/weather_profiles')

            # create is not exists
            if not writer.exists():
                writer.mkdir(exist_ok=True, parents=True)

        # use default writer with path
        if isinstance(writer, (str, PathLike)):
            writer = ParquetHandler(wdir=writer)

        # ensure inheritance
        if not isinstance(writer, IOHandler):
            raise TypeError(
                f"writer must inherit from IOHandler class: {type(writer)}"
        )

        self._writer = writer

    def __init__(
        self,
        reader: PECDReader,
        writer: IOHandler | StrOrPath | None = None,
        scaler: ProfileScaler | None = None,
        heat_demand_generator: HeatDemandProfileGenerator | None = None,
        ucf_irradiance: float | None = None,
        ucf_temperature: float | None = None,
        ucf_windspeed: float | None = None,
    ):

        # assign parameters
        self.reader = reader
        self.scaler = scaler
        self.heat_demand_generator = heat_demand_generator
        self.writer = writer

        # unit conversion factors
        self.ucf_irradiance = ucf_irradiance
        self.ucf_temperature = ucf_temperature
        self.ucf_windspeed = ucf_windspeed

    @classmethod
    def from_pecd31_csvs(
        cls,
        wdir: StrOrPath,
        mapper: WeatherProfileMapping | None = None,
        regionmapper: RegionMapping | None = None,
        writer: StrOrPath | IOHandler | None = None,
        scaler: ProfileScaler | None = None
    ):
        """CSV based PECD IO"""

        # initialise reader
        ucf_irradiance = 1 / 1e3
        logger.info("Unit conversion for PECD31 irradiance profiles required")
        reader = PECD31CSVReader(wdir=wdir, mapper=mapper, regionmapper=regionmapper)

        return cls(
            reader=reader,
            writer=writer,
            scaler=scaler,
            ucf_irradiance=ucf_irradiance
        )

    @classmethod
    def from_pecd41_csvs(
        cls,
        wdir: StrOrPath,
        mapper: WeatherProfileMapping | None = None,
        regionmapper: RegionMapping | None = None,
        writer: StrOrPath | IOHandler | None = None,
        scaler: ProfileScaler | None = None
    ):
        """CSV based PECD IO"""

        # initialise reader
        ucf_irradiance = 1 / 1e3
        logger.info("Unit conversion for PECD41 irradiance profiles required")
        reader = PECD41CSVReader(wdir=wdir, mapper=mapper, regionmapper=regionmapper)

        return cls(
            reader=reader,
            writer=writer,
            scaler=scaler,
            ucf_irradiance=ucf_irradiance
        )

    def make_capacity_factors(
        self,
        region: str,
        cyear: int,
        solar_scalar: float | None = None,
        offshore_scalar: float | None = None,
        onshore_scalar: float | None = None,
        write: bool | IOHandler = False
    ) -> pd.DataFrame:
        """make ETM capacity factors"""

        # load capacity profiles
        solar = self.reader.read_solar_pv(region=region, cyear=cyear).iloc[:8760]
        onshore = self.reader.read_wind_onshore(region=region, cyear=cyear).iloc[:8760]
        offshore = self.reader.read_wind_offshore(region=region, cyear=cyear).iloc[:8760]

        logger.info(
            f'Generating capacity factors for region={region}, cyear={cyear}.'
        )

        if solar_scalar is not None:
            logger.debug(
                "Scaling solar_pv for region=%s, cyear=%s with scalar %s",
                region, cyear, solar_scalar
            )

            solar = self.scaler.scale(solar, scalar=solar_scalar)

        if onshore_scalar is not None:
            logger.debug(
                "Scaling onshore for region=%s, cyear=%s with scalar %s",
                region, cyear, onshore_scalar
            )

            onshore = self.scaler.scale(onshore, onshore_scalar)

        if offshore_scalar is not None:
            logger.debug(
                "Scaling offshore for region=%s, cyear=%s with scalar %s",
                region, cyear, offshore_scalar
            )

            offshore = self.scaler.scale(offshore, offshore_scalar)

        cfactors = validate_capacity_factors(
            wind_coastal=onshore,
            wind_offshore=offshore,
            wind_onshore=onshore,
            solar_pv=solar,
            solar_thermal=solar
        )

        # export results
        if write is not False:

            # make writer
            writer = write if write is not True else None

            self._write_profiles(
                frame=cfactors,
                region=region,
                cyear=cyear,
                writer=writer
            )

        return cfactors

    def make_heat_demand_profiles(
        self,
        region: str,
        cyear: int,
        write: bool | IOHandler = False
    ) -> pd.DataFrame:
        """make ETM heat demand profiles"""

        # load profiles and trim to 8760 hours (leap years)
        # scale units based on unit conversion factor
        irradiance = self.reader.read_irradiance(
            region, cyear).iloc[:8760] * self.ucf_irradiance

        temperature = self.reader.read_temperature(
            region, cyear).iloc[:8760] * self.ucf_temperature

        wind_speed = self.reader.read_windspeed(
            region, cyear).iloc[:8760] * self.ucf_windspeed

        logger.info(
            f'Generating heat demand profiles for region={region}, cyear={cyear}.'
        )

        # make heat demand profiles
        qprofiles = self.heat_demand_generator.make_heat_demand_profiles(
            temperature=temperature,
            irradiance=irradiance,
            wind_speed=wind_speed
        )

        # export results
        if write is not False:

            # make writer
            writer = write if write is not True else None

            # write profiles
            self._write_profiles(
                frame=qprofiles,
                region=region,
                cyear=cyear,
                writer=writer
            )

        return qprofiles

    def make_weather_profiles(
        self,
        region: str,
        cyear: int,
        solar_scalar: float | None = None,
        offshore_scalar: float | None = None,
        onshore_scalar: float | None = None,
        write: bool | IOHandler = False,
    ) -> pd.DataFrame:
        """make ETM weather profiles"""

        # make heat demand profiles
        qprofiles = self.make_heat_demand_profiles(region=region, cyear=cyear)

        # make capacity factors
        cfactors = self.make_capacity_factors(
            region=region,
            cyear=cyear,
            solar_scalar=solar_scalar,
            offshore_scalar=offshore_scalar,
            onshore_scalar=onshore_scalar
        )

        profiles = pd.concat([qprofiles, cfactors], axis=1).sort_index(axis=1)

        # export results
        if write is not False:

            # make writer
            writer = write if write is not True else None

            # write profiles
            self._write_profiles(
                frame=profiles,
                region=region,
                cyear=cyear,
                writer=writer
            )

        return profiles

    def _write_profiles(
        self,
        frame: pd.DataFrame,
        region: str,
        cyear: int,
        writer: IOHandler | None = None,
    ):

        # default writer
        if writer is None:
            writer = self.writer

        # write profiles
        self.writer.write(
            frame=frame,
            region=region,
            cyear=cyear
        )

    def make_weather_profiles_from_frame(
        self,
        experiments: pd.DataFrame,
    ) -> None:
        """multiprocessing wrapper of write_etm_profiles for batch processing"""

        with ProcessPoolExecutor() as executor:
            for idx, experiment in experiments.iterrows():

                # execute future
                future = executor.submit(
                    functools.partial(
                        self.make_weather_profiles,
                        **experiment,
                    )
                )

                # log exceptions
                exc = future.exception()
                if exc:

                    # format exception
                    lines = format_exception_only(type(exc), exc)
                    msg = ''.join(lines).rstrip()

                    # logging events
                    logger.error("Experiment %s | %s", idx, msg)
                    logger.debug("Traceback for experiment %s:", idx, exc_info=exc)
