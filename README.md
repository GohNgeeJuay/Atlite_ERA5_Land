# Atlite using higher resolution ERA5 Dataset for PV modelling

### Author: Jake Goh
### Date: 9th February 2025


This repository is a fork of the atlite repository at https://github.com/PyPSA/atlite. 

I have extended Atlite to be able to support ERA5 Land dataset. The dataset has an improved ~9 km spatial resolution compared to the ERA5 dataset with ~31 km grid resolution.  

This repository includes the changes to the Atlite library to support the new dataset and EDA after the change to complement my blog at:  

---

### Repository Structure

atlite/

├── era5_land_EDA (this is where I perform my analysis after the extension to Atlite)
   ├── data (this contains the data downloaded from the era5_land_EDA.ipynb and other files I need for analysis/visualization)
   ├── era5_land_EDA.ipynb (notebook where the EDA is performed after ERA5 Land extension to Atlite)
├── atlite
   ├── datasets
      ├── __init__.py (where I added a new dataset to atlite supported datasets)
      ├── era5_land.py (where I added the new datasset functionality)
   ├── other atlite foler/files
├── other atlite foler/files


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







