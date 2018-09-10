#!/net/exo/landclim/crezees/conda/envs/esmval1/bin/python

import cdsapi
import time
import datetime
import subprocess
import os
import shutil
import glob

'''
# Author: Bas Crezee
# Date: 10 Sept 2018
# This script can be used to download soil moisture data from the Climate Data Store and CMORIZE it, thereby making it readable to the ESMVal tool. Note that data requests (as of Sept 2018) that are too large (e.g. > 4 years of daily data, single levels, one variable) will fail.
'''

# Parameters
dataset_tags = ['ERA5-SM-LAYER2']#'ERA5-SM-LAYER1']
# Full period for ERA5 is currently 2000-2017
startyear = 2002
endyear = 2017

# Fixed parameters
workdir = '/net/exo/landclim/PROJECTS/C3S/workdir/cds_retrieval/'
destinationdir = '/net/exo/landclim/PROJECTS/C3S/datadir/obsdir/'

data_retrieval_dict = {
'ERA5-SM-LAYER1' :  {
'cds_name' : 'reanalysis-era5-single-levels',
'cds_varname' : 'swvl1',
'cds_retrieval_dict' :         {
            'variable':'volumetric_soil_water_layer_1',
            'product_type':'reanalysis',
            'year': list(range(startyear,endyear+1)),
            'month':list(range(1,13)),
            'day': list(range(1,32)),
            'time':['00:00','06:00','12:00','18:00'],
            'format':'netcdf'
        }
},
'ERA5-SM-LAYER2' :  {
'cds_name' : 'reanalysis-era5-single-levels',
'cds_varname' : 'swvl2',
'cds_retrieval_dict' :         {
            'variable':'volumetric_soil_water_layer_2',
            'product_type':'reanalysis',
            'year': list(range(startyear,endyear+1)),
            'month':list(range(1,13)),
            'day': list(range(1,32)),
            'time':['12:00'],
            'format':'netcdf'
        }
}
}


for dataset_tag in dataset_tags:
    
    # Start a timer to measure processing time
    t_start = time.time()
    
    # Create the NC filename, in the ESMVal format
    ncfile_tail = 'OBS_{0}_ground_L3_T2Is_sm_{1}01-{2}12.nc'.format(dataset_tag,startyear,endyear)
    # Create the full path
    fullpath = os.path.join(workdir,ncfile_tail)
    
    # Initialize the cds client (note that a config file needs to exist in your home directory)
    # see: https://pypi.org/project/cdsapi/
    cds = cdsapi.Client()
    # Create a data request
    datarequest = cds.retrieve(
    	      data_retrieval_dict[dataset_tag]['cds_name'],
    	      data_retrieval_dict[dataset_tag]['cds_retrieval_dict'])
    # Download the request
    datarequest.download(fullpath)
    
    # End timer
    t_end = time.time()
    print("Retrieved data in {0}".format(datetime.timedelta(seconds=(t_end-t_start))))
    print("Data has been saved as: {0}".format(fullpath))

    # CMORIZE data making use of ncrename/ncatted as subprocesses
    print("Start CMORIZING")
    cmd_list = ['ncrename','-v','{0},sm'.format(data_retrieval_dict[dataset_tag]['cds_varname']),fullpath]
    print("$ {0}".format(' '.join(cmd_list)))
    subprocess.call(cmd_list)
    cmd_list = ['ncatted','-a','''long_name,sm,o,c,Volumetric Soil Moisture''',fullpath] 
    print("$ {0}".format(' '.join(cmd_list)))
    subprocess.call(cmd_list)
    cmd_list = ['ncatted','-a','''units,sm,m,c,m3 m-3''',fullpath]
    print("$ {0}".format(' '.join(cmd_list)))
    subprocess.call(cmd_list)
    # Move file to the ESMVal obsdir (overwrites if it exists already)
    print("Moving file to the ESMVal directory")
    shutil.move(fullpath,os.path.join(destinationdir,ncfile_tail))
    print("Finished this file.")

