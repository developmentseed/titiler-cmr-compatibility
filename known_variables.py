known_variables = set(["Tair", "SnowDepth", "Rainf", "Snowf_tavg", "wind"] + \
    ["aerosol_optical_thickness_ocean", "ColumnAmountNO2", "precip"] + \
    ["EVAP", "TS", "precipitation", "srad", "SO2", "O3", "xco2", "prec"] + \
    ["SnowDepth_tavg", "tmax", "SO2CMASS", "sma", "soil_moisture_c1", "tas"])

known_bands = set(["Red", "Green", "Blue", "Cirrus", "Swir", "Band 1"] + \
                  ["Red_Edge1", "Red_Edge2", "Red_Edge3", "TIRS1", "SWIR1", "SWIR2"] + \
                  ["Water_Vapor", "NIR_Narrow", "Coastal_Aerosol"])
