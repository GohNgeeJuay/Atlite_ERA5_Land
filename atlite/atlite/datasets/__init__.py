# SPDX-FileCopyrightText: Contributors to atlite <https://github.com/pypsa/atlite>
#
# SPDX-License-Identifier: MIT

"""
atlite datasets.
"""

from atlite.datasets import era5, gebco, sarah, era5_land

modules = {"era5": era5, "sarah": sarah, "gebco": gebco, "era5_land": era5_land}
