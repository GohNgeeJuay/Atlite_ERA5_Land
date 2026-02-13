# Atlite using higher resolution ERA5 Dataset for PV modelling

### Author: Jake Goh
### Date: 9th February 2025


This repository is a fork of the atlite repository at https://github.com/PyPSA/atlite. 

I have extended Atlite to be able to support ERA5 Land dataset. The dataset has an improved ~9 km spatial resolution compared to the ERA5 dataset with ~31 km grid resolution.  

This repository includes the changes to the Atlite library to support the new dataset and EDA after the change to complement my blog at:  

---

### Repository Structure

atlite/
├── era5_land_EDA/                          # Analysis after extending Atlite
│   ├── data/                               # Downloaded ERA5-Land data + files for plots
│   └── era5_land_EDA.ipynb                 # Notebook for post-extension EDA
│
├── atlite/
│   ├── datasets/
│   │   ├── __init__.py                     # Added ERA5-Land to supported datasets registry
│   │   └── era5_land.py                    # New ERA5-Land dataset module implementation
│   │
│   └── ...                                 # Other Atlite files
│
└── ...                                     # Other project files


To reproduce the findings in the notebooks:

## Set up
1. Clone the repository
   `git clone https://github.com/GohNgeeJuay/Atlite_Era5_Land.git`
2. Create conda environment:
   ```
   conda env create -f environment.yml
   conda activate atlite-era5-land
   ```
3. To replicate the findings, run the `era5_land_EDA.ipynb`

## Contact
GitHub: https://github.com/gohngeejuay
Linkedin: www.linkedin.com/in/gohngeejuay

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.







