# Multi-DEM-conversion
A command line tool using ArcPy or pyGDAL to convert DEM to slope or other raster.

## Using ArcPy -  	ArcpyMultiProcessTool.py
This is a template of how to use arcpy with python multiprocess module to process a bunch of files with same process (eg. DEM -> Slope raster)
Developement is under Windows 10 with ArcGIS 10.3 (Python 2.7.10 32-bit)
Remember to use the ArcGIS built-in python to run the script otherwise Arcpy may not correctly functioned.

Syntax:
        $python ArcpyMultiProcessTool.py \Directory\ mode (-e=format) (-t=Processes)
*   \Directory\ : All the raster in the directiry with specific extension will be processed.
*   mode : Currently only 'Slope' and 'Hillshade' was allowed
*   (-e=format) : 'tif' by default. You can specify which format of raster is going to be processed.
*   (-t=Processes) : 2 by default. You can specify how many threads will be cteated to run the specific function. If you give a number larger than your cpu (threads) number, it will raise a error.
