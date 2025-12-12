# Unequal Nights - Notebook Documentation

## Project Overview
Philadelphia Lighting Equity Analysis: Identifying underserved neighborhoods through satellite data, 311 complaints, and spatial analysis.

---

## Notebook Workflow

### 01_VIIRS_Data_Extraction_Updated.ipynb
**Purpose:** Extract NASA VIIRS Black Marble nighttime lights satellite data

**Key Tasks:**
- Download VIIRS DNB (Day/Night Band) data for Philadelphia (2022-2024)
- Process HDF5 files to extract radiance values
- Clip to Philadelphia boundary
- Export raster for integration

**Output:** VIIRS radiance rasters by year

---

### 02_Data_Integration.ipynb
**Purpose:** Integrate multiple data sources into unified spatial dataset

**Key Tasks:**
- Load Philadelphia block groups (Census boundaries)
- Join VIIRS brightness values (zonal statistics)
- Join 311 streetlight complaints (spatial join)
- Join crime data (nighttime incidents 7pm-6am)
- Join Census demographics (income, population)
- Normalize all variables to 0-1 scale

**Output:** `philly_integrated_data.shp` - Master dataset with all variables

---

### 03_Complaint_Gap_Analysis_FIXED.ipynb
**Purpose:** Identify "Complaint Gap" areas - dark neighborhoods with low 311 reporting

**Key Tasks:**
- Define gap criteria: Low VIIRS + Low 311 + Low Income
- Spatial overlay to identify gap areas
- Calculate gap area statistics
- Analyze demographic patterns in gap vs non-gap areas

**Output:** 
- `complaint_gap_areas.shp` - 62 identified gap areas
- Gap area statistics and visualizations

---

### 04_Predictive_Modeling_CLEAN.ipynb
**Purpose:** Temporal trend analysis using linear regression

**Key Tasks:**
- Calculate brightness trends (2022-2024 slope)
- Classify areas: Darkening Fast / Darkening / Stable / Brightening
- Identify 399 areas showing darkening trend (31.1%)
- Optional: Moran's I spatial autocorrelation analysis

**Output:**
- `final_analysis_with_trends.shp`
- `darkening_trend_areas.shp` - 399 areas
- `temporal_trends_clean.png`
- `darkening_areas_map.png`

**Note:** This version uses legitimate trend analysis instead of problematic Random Forest prediction

---

### 05_Export_for_ArcGIS.ipynb
**Purpose:** Prepare data for ArcGIS Pro analysis

**Key Tasks:**
- Export shapefiles with proper field names (10-char limit)
- Create separate layers for different analyses
- Verify coordinate systems (EPSG:4326 / EPSG:2272)

**Output:** ArcGIS-ready shapefiles

---

### 06_Network_Safe_Path_FINAL.ipynb
**Purpose:** Network-based safe route analysis using OSMnx and NetworkX

**Key Tasks:**
- Download Philadelphia street network from OpenStreetMap
- Assign risk weights to edges based on cost surface
- Calculate shortest vs safest routes between key locations
- Compare distance vs risk trade-offs

**Output:**
- Route comparisons (52 routes analyzed)
- Risk reduction statistics (up to 20% safer with 1-3% longer distance)

---

### 06_Street_Poles_Grid.ipynb
**Purpose:** Analyze streetlight pole distribution

**Key Tasks:**
- Process streetlight pole location data
- Create spatial grid for density analysis
- Identify areas with low pole coverage

**Output:** Streetlight density analysis

---

### 07_Block_Classification_FIXED.ipynb
**Purpose:** Classify block groups by priority level

**Key Tasks:**
- Combine multiple risk factors
- Create priority classification (Critical / High / Medium / Low)
- Generate final priority rankings

**Output:** Priority-classified block groups

---

## Data Sources
- **VIIRS:** NASA Black Marble VNP46A2 (2022-2024)
- **311:** Philadelphia Open Data - Streetlight complaints
- **Crime:** PennDOT crash data / Philadelphia Police
- **Census:** ACS 5-year estimates (income, demographics)
- **Streets:** OpenStreetMap via OSMnx

## Key Findings
- **62** Complaint Gap areas identified
- **399** areas (31.1%) showing darkening trend
- **76%** fewer complaints in darkest neighborhoods
- Safe routes reduce risk by up to **20%** with minimal detour

---