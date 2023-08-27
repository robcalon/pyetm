"""Profile smoother, adapted from:
https://github.com/quintel/etdataset-public/blob/master/curves/demand/households/space_heating/script/smoothing.pys"""

import numpy as np


class ProfileSmoother:
    """
    The profiles generator is based on data for an individual household.
    To transform this into a profile for a typical neighbourhood (e.g. 300 houses)
    we shift the heat demand curves multiple times based on a normal distribution
    and return the average profile
    The following steps are taken:
    1. The original heat demand curves are interpolated with a step size of 10.
    This simulates a curve with 6 minute time intervals rather than a 1 hour.
    2. X random numbers are drawn based on a normal distribution with mean 0
    and an Y-hour standard deviation. These numbers are rounded to 1 decimal point
    and multiplied by 10. Each number represents the number of 6 minute intervals
    deviation from the mean.
    3. The original heat demand curve is loaded 300 times and shifted Z intervals
    backwards or forwards in time depending on the value of the random numbers.
    This results in 300 demand curves that follow the exact same pattern
    over time, but with a different starting point. Depending on the value of the
    random number a curve starts earlier or later than the original curve
    4. The X curves are summed and converted back to a 1 hour interval.
    This results in a curve that represents the aggregated/average heat demand of
    X houses rather than an individual household.
    """

    def __init__(
        self,
        number_of_houses=300,
        hours_shifted=None,
        interpolation_steps=10,
        random_seed=1337,
    ):
        """init"""

        # Standard deviation per insulation type.
        # See README for source
        if hours_shifted is None:
            hours_shifted = {"low": 2, "medium": 2.5, "high": 3}

        # interpolation steps
        # use intervals of 6 minutes when shifting curves

        self.number_of_houses = number_of_houses
        self.hours_shifted = hours_shifted
        self.interpolation_steps = interpolation_steps
        self.random_seed = random_seed

    def generate_deviations(self, size, scale):
        """
        Generate X random numbers with a standard deviation of Y hours
        Round to 1 decimal place and multiply by 10 to get
        integer value. The number designates the number of
        6 minute time slots the demand profile will be shifted
        compared to the original demand profile.
        E.g. '15' means that the demand profile will be shifted
        forward 1.5 hours, '-10' means it will be shifted backwards 1 hour
        """
        # (re)set random seed
        np.random.seed(self.random_seed)

        # generate X random numbers with normal distribution
        random_numbers = np.random.normal(loc=0.0, scale=scale, size=size)

        # round by 1 decimal point
        rounded_numbers = np.round(random_numbers, 1)

        # multiply by 10 to get integer numbers for the deviations
        shifts = rounded_numbers * 10

        return shifts.astype(int)

    def interpolate(self, arr, steps):
        """
        Interpolate the original demand profile
        to allow for smaller intervals than 1
        hour (steps=10 means 6 minute intervals)
        """
        interpolated_arr = []
        for index, value in enumerate(arr):
            start = value
            if index == len(arr) - 1:
                stop = arr[0]
            else:
                stop = arr[index + 1]

            step_size = (stop - start) / steps
            for i in range(0, steps):
                interpolated_arr.append(start + i * step_size)

        return interpolated_arr

    def shift_curve(self, arr, num):
        """
        Rotate the elements of an array based on num.
        Example: if num = 5, each element will be shifted 5 places forwards.
        Elements at the end of the array will be put at the front.
        """
        return np.roll(arr, num)

    def trim_interpolated(self, arr, steps):
        """
        Converts the curve back to the original number of data points (8760) by
        taking the average of X data points before and X data points after each
        hour (where X = INTERPOLATION_STEPS)
        Example: If INTERPOLATION_STEPS = 10, the interpolated curve has 87600 data
        points. Trimming it results in a curve with 8760 data points, where each
        data point (hour) is the average of 30 minutes before and after the whole
        hour.
        """
        arr = self.shift_curve(arr, self.interpolation_steps // 2)
        return [sum(arr[i : (i + steps)]) / steps for i in range(0, len(arr), steps)]

    def calculate_smoothed_demand(self, heat_demand, insulation_type):
        """calculate smoothed demand"""

        # start out with list of zeroes
        cumulative_demand = [0] * len(heat_demand) * self.interpolation_steps

        # generate random numbers
        deviations = self.generate_deviations(
            self.number_of_houses, self.hours_shifted[insulation_type]
        )

        # interpolate the demand curve to increase the number of data points
        # (i.e. reduce the time interval 1 hour to e.g. 6 minutes)
        interpolated_demand = self.interpolate(heat_demand, self.interpolation_steps)

        # for each random number, shift the demand curve X places forwards or
        # backwards (depending on the number value) and add it to the
        # cumulative demand array
        for num in deviations:
            demand_list = self.shift_curve(interpolated_demand, num)
            cumulative_demand = [x + y for x, y in zip(cumulative_demand, demand_list)]

        # Trim the cumulative demand array such that it has 8760 data points again
        # (hourly intervals instead of 6 minute intervals)
        smoothed_demand = self.trim_interpolated(
            cumulative_demand, self.interpolation_steps
        )

        return smoothed_demand
