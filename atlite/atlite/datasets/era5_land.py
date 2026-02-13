# SPDX-FileCopyrightText: Contributors to atlite <https://github.com/pypsa/atlite>
#
# SPDX-License-Identifier: MIT
"""
Module for downloading and curating data from ECMWFs ERA5 dataset (via CDS).

For further reference see
https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation
"""

import logging
import os
import warnings
import weakref
from pathlib import Path
from tempfile import mkstemp

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
from dask import compute, delayed
from dask.array import arctan2, sqrt
from dask.utils import SerializableLock
from numpy import atleast_1d

from atlite.gis import maybe_swap_spatial_dims
from atlite.pv.solar_position import SolarPosition

# Null context for running a with statements wihout any context
try:
    from contextlib import nullcontext
except ImportError:
    # for Python verions < 3.7:
    import contextlib

    @contextlib.contextmanager
    def nullcontext():
        yield


logger = logging.getLogger(__name__)

# Model and CRS Settings
crs = 4326


#TOA calculation
import numpy as np
import xarray as xr
import pandas as pd



def _convert_to_radians(dataset : xr.Dataset) ->  xr.Dataset:

    dataset["y_rad"] = np.deg2rad(dataset["y"])
    dataset["x_rad"] = np.deg2rad(dataset["x"])

    return dataset


def _calc_day_of_year(dataset : xr.Dataset) ->  xr.Dataset:

    dataset["day_of_year"] = dataset["time"].dt.dayofyear
    return dataset


def _calc_eccentricty_correlation_factor(dataset : xr.Dataset) ->  xr.Dataset:

    dataset["ECF"] = xr.DataArray(
        1 + 0.0333 * np.cos( np.deg2rad( (360*dataset["day_of_year"])/365)),
        dims=["time"],
        coords={"time": dataset["time"]}
    )
    return dataset



def _calc_hour_angle(dataset : xr.Dataset) ->  xr.Dataset:
    """
    At LST = 12, HRA = 0° → solar noon
    At LST < 12, HRA is negative → morning (Sun east of meridian)
    At LST > 12, HRA is positive → afternoon (Sun west of meridian)
    """
    
    #Reference for calculating local solar time: https://www.pveducation.org/pvcdrom/properties-of-sunlight/solar-time
    #TODO the timezone has to be dynamic if we are calculating the hour angle for regions outside GMT+8
    time_utc = dataset["time"]
    #Tz_localize add the UTC, tz_convert converts the time into a specific time zone. 
    time_local = pd.to_datetime(time_utc).tz_localize("UTC").tz_convert("Asia/Kuala_Lumpur")

    time_zone = 8 #Need to pass in with the Asia/Kuala Lumpur
    # #calculate local standard time meridian (LSTM)
    LSTM = 15*time_zone
    
    #calculate local time in hours
    dataset["LT"] = xr.DataArray(
        time_local.hour + time_local.minute/60 + time_local.second/3600,
        dims=["time"],
        coords={"time": dataset["time"]}
    )

    #Calculate parameter B
    B_param = 360/365*(dataset["day_of_year"]-81)
    dataset["B_param_rad"] = xr.DataArray(
        np.deg2rad(B_param),
        dims=["time"],
        coords={"time": dataset["time"]}
    )
    
    #calculate equation of time (EOT)
    dataset["EOT"] = xr.DataArray(
        9.87*np.sin(2*dataset["B_param_rad"]) - 7.53*np.cos(dataset["B_param_rad"]) - 1.5*np.sin(dataset["B_param_rad"]),
        dims=["time"],
        coords={"time": dataset["time"]}
    )

    dataset["TC"] = 4 * (dataset["x"] - LSTM) + dataset["EOT"]     

    #calculate local solar time (LST)
    dataset["LST"] = xr.DataArray(
        dataset["LT"] + (dataset["TC"]/60), 
        dims = ["time", "x"],
        coords = {"time": dataset["time"], "x": dataset["x"]}
    )
    
    #calculate hour angle (HRA). Convert the final result into radians. Previous step should not convert into radians.
    dataset["hour_angle"] = xr.DataArray(
        np.deg2rad(15*(dataset["LST"] - 12)),
        dims = ["time", "x"],
        coords = {"time": dataset["time"], "x": dataset["x"]}
    )

    return dataset


def _calc_solar_declination_angle(dataset : xr.Dataset) ->  xr.Dataset:

    sda_deg = 23.45 * np.sin(np.deg2rad((360/365) * (dataset["day_of_year"] - 81)))
    dataset["SDA"] = xr.DataArray(
        np.deg2rad(sda_deg),
        dims=["time"],
        coords={"time": dataset["time"]}
    )

    return dataset



def _calc_solar_zenith_angle(dataset: xr.Dataset) -> xr.Dataset:
    
    lat = np.deg2rad(dataset["y"])  
    sda = dataset["SDA"]
    ha = dataset["hour_angle"]

    cos_sza = (
        np.sin(lat) * np.sin(sda)
        + np.cos(lat) * np.cos(sda) * np.cos(ha)
    )

    cos_sza = cos_sza.clip(-1, 1)
    dataset["SZA"] = np.arccos(cos_sza)
    return dataset



def _calc_toa_incident_shortwave(dataset: xr.Dataset) ->  xr.Dataset:

    influx = 1361 * dataset["ECF"] * np.cos(dataset["SZA"])
    dataset["influx_toa"] = influx.clip(min=0)

    return dataset


def _calculate_toa_influx(dataset: xr.Dataset) ->  xr.Dataset:

    """
    Will mutate the given dataset and provide a new variable 'influx_toa' which is the top of atmosphere solar influx.
    
    Parameters
    ----------
    dataset : xarray.Dataset
        The input dataset
    
    Returns
    -------
    xarray.Dataset
        The mutated dataset with the new variable 'influx_toa'
    """

    # Solar geometry pipeline
    for func in (
        _convert_to_radians,
        _calc_day_of_year,
        _calc_eccentricty_correlation_factor,
        _calc_hour_angle,
        _calc_solar_declination_angle,
        _calc_solar_zenith_angle,
        _calc_toa_incident_shortwave
    ):
        dataset = func(dataset)

    #Clean up by deleting intermediate variables
    dataset = dataset.drop_vars(["ECF", "SZA", "SDA", "hour_angle", "LT", "B_param_rad", "EOT", "TC", "LST", "day_of_year", "x_rad", "y_rad"], errors="ignore")

    return dataset


##########

#TODO Only supporting influx and temperature right now. Can add more later
features = {
    "influx": [
        "influx_toa",
        "influx",
        "albedo",
        "solar_altitude",
        "solar_azimuth",
    ],
    "temperature": ["temperature", "dewpoint temperature"]
}

static_features = {"height"}


def _rename_and_clean_coords(ds, add_lon_lat=True):
    """
    Rename 'longitude' and 'latitude' columns to 'x' and 'y' and fix roundings.

    Optionally (add_lon_lat, default:True) preserves latitude and
    longitude columns as 'lat' and 'lon'.
    """
    ds = ds.rename({"longitude": "x", "latitude": "y", "valid_time": "time"})
    # round coords since cds coords are float32 which would lead to mismatches
    ds = ds.assign_coords(
        x=np.round(ds.x.astype(float), 5), y=np.round(ds.y.astype(float), 5)
    )
    ds = maybe_swap_spatial_dims(ds)
    if add_lon_lat:
        ds = ds.assign_coords(lon=ds.coords["x"], lat=ds.coords["y"])
    ds = ds.drop_vars(["expver", "number"], errors="ignore")

    return ds

def get_data_influx(retrieval_params):
    """
    Get influx data for given retrieval parameters.
    """
    ds = retrieve_data(
        variable=[
            "surface_net_solar_radiation",  #SSR
            "surface_solar_radiation_downwards", #SSRD
        ],
        **retrieval_params,
    )

    #Atlite conversions of ERA5 data to their convention
    ds = _rename_and_clean_coords(ds)

    ds["albedo"] = (
        ((ds["ssrd"] - ds["ssr"]) / ds["ssrd"].where(ds["ssrd"] != 0))
        .fillna(0.0)
        .assign_attrs(units="(0 - 1)", long_name="Albedo")
    )

    # ERA5 variables are mean values for previous hour, i.e. 13:01 to 14:00 are labelled as "14:00"
    # account by calculating the SolarPosition for the center of the interval for aggregation happens
    # see https://github.com/PyPSA/atlite/issues/158
    # Do not show DeprecationWarning from new SolarPosition calculation (#199)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        time_shift = pd.to_timedelta("-30 minutes")
        sp = SolarPosition(ds, time_shift=time_shift)
    sp = sp.rename({v: f"solar_{v}" for v in sp.data_vars})

    ds = xr.merge([ds, sp])

    ds["influx"] = ds["ssrd"]/3600
    ds = _calculate_toa_influx(ds) 

    #Unused vars
    ds = ds.drop_vars(["ssrd", "ssr"])
    return ds


def sanitize_influx(ds):
    """
    Sanitize retrieved influx data.
    """
    influx_variables = ["influx", "influx_direct", "influx_diffuse", "influx_toa"]

    #TODO: add a try block here to ignore non existing keys
    for var in influx_variables:
        try:
            ds[var] = ds[var].clip(min=0.0)
        except KeyError:
            continue
    return ds


def get_data_temperature(retrieval_params):
    """
    Get wind temperature for given retrieval parameters.
    """
    ds = retrieve_data(
        variable=[
            "2m_temperature",
            "2m_dewpoint_temperature",
        ],
        **retrieval_params,
    )

    ds = _rename_and_clean_coords(ds)
    ds = ds.rename(
        {
            "t2m": "temperature",
            "d2m": "dewpoint temperature",
        }
    )

    return ds



def _area(coords):
    # North, West, South, East. Default: global
    x0, x1 = coords["x"].min().item(), coords["x"].max().item()
    y0, y1 = coords["y"].min().item(), coords["y"].max().item()
    return [y1, x0, y0, x1]


def retrieval_times(coords, static=False, monthly_requests=False):
    """
    Get list of retrieval cdsapi arguments for time dimension in coordinates.

    If static is False, this function creates a query for each month and year
    in the time axis in coords. This ensures not running into size query limits
    of the cdsapi even with very (spatially) large cutouts.
    If static is True, the function return only one set of parameters
    for the very first time point.

    Parameters
    ----------
    coords : atlite.Cutout.coords
    static : bool, optional
    monthly_requests : bool, optional
        If True, the data is requested on a monthly basis. This is useful for
        large cutouts, where the data is requested in smaller chunks. The
        default is False

    Returns
    -------
    list of dicts witht retrieval arguments

    """
    time = coords["time"].to_index()
    if static:
        return {
            "year": [time[0].strftime("%Y")],
            "month": [time[0].strftime("%m")],
            "day": [time[0].strftime("%d")],
            "time": time[0].strftime("%H:00"),
        }

    # Prepare request for all months and years
    times = []
    for year in time.year.unique():
        t = time[time.year == year]
        if monthly_requests:
            for month in t.month.unique():
                query = {
                    "year": [str(year)],
                    "month": [t[t.month == month][0].strftime("%m")],
                    "day": list(t[t.month == month].strftime("%d").unique()),
                    "time": list(t[t.month == month].strftime("%H:00").unique()),
                }
                times.append(query)
        else:
            query = {
                "year": [str(year)],
                "month": list(t.strftime("%m").unique()),
                "day": list(t.strftime("%d").unique()),
                "time": list(t.strftime("%H:00").unique()),
            }
            times.append(query)
    return times


def noisy_unlink(path):
    """
    Delete file at given path.
    """
    logger.debug(f"Deleting file {path}")
    try:
        os.unlink(path)
    except PermissionError:
        logger.error(f"Unable to delete file {path}, as it is still in use.")


def add_finalizer(ds: xr.Dataset, target: str | Path):
    logger.debug(f"Adding finalizer for {target}")
    weakref.finalize(ds._close.__self__.ds, noisy_unlink, target)


def sanitize_chunks(chunks, **dim_mapping):
    dim_mapping = dict(time="valid_time", x="longitude", y="latitude") | dim_mapping
    if not isinstance(chunks, dict):
        # preserve "auto" or None
        return chunks

    return {
        extname: chunks[intname]
        for intname, extname in dim_mapping.items()
        if intname in chunks
    }


def open_with_grib_conventions(
    grib_file: str | Path, chunks=None, tmpdir: str | Path | None = None
) -> xr.Dataset:
    """
    Convert grib file of ERA5 data from the CDS to netcdf file.

    The function does the same thing as the CDS backend does, but locally.
    This is needed, as the grib file is the recommended download file type for CDS, with conversion to netcdf locally.
    The routine is a reduced version based on the documentation here:
    https://confluence.ecmwf.int/display/CKB/GRIB+to+netCDF+conversion+on+new+CDS+and+ADS+systems#GRIBtonetCDFconversiononnewCDSandADSsystems-jupiternotebook

    Parameters
    ----------
    grib_file : str | Path
        Path to the grib file to be converted.
    chunks
        Chunks
    tmpdir : Path, optional
        If None adds a finalizer to the dataset object

    Returns
    -------
    xr.Dataset
    """
    #
    # Open grib file as dataset
    # Options to open different datasets into a datasets of consistent hypercubes which are compatible netCDF
    # There are options that might be relevant for e.g. for wave model data, that have been removed here
    # to keep the code cleaner and shorter
    ds = xr.open_dataset(
        grib_file,
        engine="cfgrib",
        time_dims=["valid_time"],
        ignore_keys=["edition"],
        # extra_coords={"expver": "valid_time"},
        coords_as_attributes=[
            "surface",
            "depthBelowLandLayer",
            "entireAtmosphere",
            "heightAboveGround",
            "meanSea",
        ],
        chunks=sanitize_chunks(chunks),
    )
    if tmpdir is None:
        add_finalizer(ds, grib_file)

    def safely_expand_dims(dataset: xr.Dataset, expand_dims: list[str]) -> xr.Dataset:
        """
        Expand dimensions in an xarray dataset, ensuring that the new dimensions are not already in the dataset
        and that the order of dimensions is preserved.
        """
        dims_required = [
            c for c in dataset.coords if c in expand_dims + list(dataset.dims)
        ]
        dims_missing = [
            (c, i) for i, c in enumerate(dims_required) if c not in dataset.dims
        ]
        dataset = dataset.expand_dims(
            dim=[x[0] for x in dims_missing], axis=[x[1] for x in dims_missing]
        )
        return dataset

    logger.debug("Converting grib file to netcdf format")
    # Variables and dimensions to rename if they exist in the dataset
    rename_vars = {
        "time": "forecast_reference_time",
        "step": "forecast_period",
        "isobaricInhPa": "pressure_level",
        "hybrid": "model_level",
    }
    rename_vars = {k: v for k, v in rename_vars.items() if k in ds}
    ds = ds.rename(rename_vars)

    # safely expand dimensions in an xarray dataset to ensure that data for the new dimensions are in the dataset
    ds = safely_expand_dims(ds, ["valid_time", "pressure_level", "model_level"])

    return ds


def retrieve_data(
    product: str,
    chunks: dict[str, int] | None = None,
    tmpdir: str | Path | None = None,
    lock: SerializableLock | None = None,
    **updates,
) -> xr.Dataset:
    """
    Download data like ERA5 from the Climate Data Store (CDS).

    If you want to track the state of your request go to
    https://cds-beta.climate.copernicus.eu/requests?tab=all

    Parameters
    ----------
    product : str
        Product name, e.g. 'reanalysis-era5-land'.
    chunks : dict, optional
        Chunking for xarray dataset, e.g. {'time': 1, 'x': 100, 'y': 100}.
        Default is None.
    tmpdir : str, optional
        Directory where the downloaded data is temporarily stored.
        Default is None, which uses the system's temporary directory.
    lock : dask.utils.SerializableLock, optional
        Lock for thread-safe file writing. Default is None.
    updates : dict
        Additional parameters for the request.
        Must include 'year', 'month', and 'variable'.
        Can include e.g. 'data_format'.

    Returns
    -------
    xarray.Dataset
        Dataset with the retrieved variables.

    Examples
    --------
    >>> ds = retrieve_data(
    ...     product='reanalysis-era5-land',
    ...     chunks={'time': 1, 'x': 100, 'y': 100},
    ...     tmpdir='/tmp',
    ...     lock=None,
    ...     year='2020',
    ...     month='01',
    ...     variable=['10m_u_component_of_wind', '10m_v_component_of_wind'],
    ...     data_format='netcdf'
    ... )
    """

    #Set up request body
    request = {"product_type": ["reanalysis"], "download_format": "unarchived"}
    request.update(updates)

    #Require year, month, day and variable as part of request
    assert {"year", "month", "day", "variable"}.issubset(request), (
        "Need to specify at least 'variable', 'year' and 'month'"
    )

    logger.debug(f"Requesting {product} with API request: {request}")

    #Creating the request object
    client = cdsapi.Client(
        info_callback=logger.debug, debug=logging.DEBUG >= logging.root.level
    )
    result = client.retrieve(product, request)

    if lock is None:
        lock = nullcontext()

    suffix = f".{request['data_format']}"  # .netcdf or .grib

    #TODO: best practice: to log the metadata of the request object.  
    #Download the data. 
    with lock:
        fd, target = mkstemp(suffix=suffix, dir=tmpdir)
        os.close(fd)

        # Inform user about data being downloaded as "* variable (year-month)"
        timestr = f"{request['year']}-{request['month']}"
        variables = atleast_1d(request["variable"])
        varstr = "\n\t".join([f"{v} ({timestr})" for v in variables])
        logger.info(f"CDS: Downloading variables\n\t{varstr}\n")

        #Not sure if this is necessary
        result.download(target)

    # Convert from grib to netcdf locally, same conversion as in CDS backend
    if request["data_format"] == "grib":
        ds = open_with_grib_conventions(target, chunks=chunks, tmpdir=tmpdir)
    else:
        ds = xr.open_dataset(target, chunks=sanitize_chunks(chunks))
        if tmpdir is None:
            add_finalizer(target)

    return ds


def get_data(
    cutout,
    feature,
    tmpdir,
    lock=None,
    data_format="grib",
    monthly_requests=False,
    concurrent_requests=False,
    **creation_parameters,
):
    """
    Retrieve data from ECMWFs ERA5 dataset (via CDS).

    This front-end function downloads data for a specific feature and formats
    it to match the given Cutout.

    Parameters
    ----------
    cutout : atlite.Cutout
    feature : str
        Name of the feature data to retrieve. Must be in
        `atlite.datasets.era5.features`
    tmpdir : str/Path
        Directory where the temporary netcdf files are stored.
    monthly_requests : bool, optional
        If True, the data is requested on a monthly basis in ERA5. This is useful for
        large cutouts, where the data is requested in smaller chunks. The
        default is False
    data_format : str, optional
        The format of the data to be downloaded. Can be either 'grib' or 'netcdf',
        'grib' highly recommended because CDSAPI limits request size for netcdf.
    concurrent_requests : bool, optional
        If True, the monthly data requests are posted concurrently.
        Only has an effect if `monthly_requests` is True.
    **creation_parameters :
        Additional keyword arguments. The only effective argument is 'sanitize'
        (default True) which sets sanitization of the data on or off.

    Returns
    -------
    xarray.Dataset
        Dataset of dask arrays of the retrieved variables.

    """
    coords = cutout.coords

    sanitize = creation_parameters.get("sanitize", True)

    retrieval_params = {
        "product": "reanalysis-era5-single-levels",
        "area": _area(coords),
        "chunks": cutout.chunks,
        "grid": f"{cutout.dx}/{cutout.dy}",
        "tmpdir": tmpdir,
        "lock": lock,
        "data_format": data_format,
    }

    func = globals().get(f"get_data_{feature}")
    sanitize_func = globals().get(f"sanitize_{feature}")

    logger.info(f"Requesting data for feature {feature}...")

    def retrieve_once(time):
        ds = func({**retrieval_params, **time})
        if sanitize and sanitize_func is not None:
            ds = sanitize_func(ds)
        return ds

    if feature in static_features:
        return retrieve_once(retrieval_times(coords, static=True)).squeeze()

    time_chunks = retrieval_times(coords, monthly_requests=monthly_requests)
    if concurrent_requests:
        delayed_datasets = [delayed(retrieve_once)(chunk) for chunk in time_chunks]
        datasets = compute(*delayed_datasets)
    else:
        datasets = map(retrieve_once, time_chunks)

    return xr.concat(datasets, dim="time").sel(time=coords["time"])
