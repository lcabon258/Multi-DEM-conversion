# -*- coding: utf-8 -*-
_Release = "MultiProcessing Release V0.1"
_ReleaseDate = "2016/12/01"
_Authur = "Cheng-Wei Sun"
"""
last edited 2016-12-01 2200 @ Mac
@author: Sun, Cheng-Wei

Tested platform:
# macOS Sierra 10.12.1
# Python 3.5 Anaconda custom (x86_64) | GCC 4.2.1 Compatible Apple LLVM 4.2 (clang-425.0.28)
# pygdal 2.1.1 (conda-forge)

Useage : 
$python pyGDAL-MultiprocessingTool.py /Directory/ mode (threads)
Arguments:
/Directory/ = Which directory contains the rasters you want to process
mode = 'slope' or 'hillshade'
threads = how many threads you want to generate to process the rasters.

"""
print("\nDEM Processing\n{} by {}".format(_Release,_ReleaseDate,_Authur))
#===== Built-in libraries =====
from glob import  glob
import os
import os.path as oph
import sys
#from multiprocessing import Pool
import multiprocessing as mps
import time
#===== Other libraries =====
from osgeo import gdal
#===== Global Variable =====
Threats = 2
Mode = ""
Directory = ""
OutputDir = None

#======== Timer ==========
class Timer():
    def __init__(self):
        start=0.0
        now=0.0
    def start(self):
        self.start=time.time()
    def exec_time(self):
        self.now=time.time()
        return (self.now-self.start)
#======== File Load Functions ==========
def dirloader(dirpath,extension="*"):
    '''
    Purpose : spcify a directory then return a list  with all files.
    input : a directory (string), file extention (string)
    output : a list of file matches the condition
    libraries used : os.path (built-in) , glob (built-in)
    '''
    if sys.platform == "win32" and not dirpath.endswith("\\") :
        pass        
        #dirpath =  dirpath+"\\" 
    dire=oph.abspath(dirpath)
    print("glob path : {}".format(oph.join(dire,"*."+extension)))
    FileList=glob(oph.join(dire,"*."+extension))
    return FileList
	
 
def MakeSlope(input_file):
    '''
    Use GDAL module to generate Slope raster.
    ref : http://gdal.org/python/
    '''
    
    #input_Raster = gdal.Open(input_file)
    #OutRaster = input_file.split(".")[0]+"_slp.tif"
    OutRaster = oph.join(OutputDir,oph.basename(input_file).split(".")[0]+"_slp.tif")
    print("Processing : {}".format(OutRaster))
    gdal.DEMProcessing(OutRaster,input_file,"slope")

def SequenceMakeSlope(input_file):
    for i in (input_file):
        MakeSlope(i)

def MakeHillshade(input_file):
    '''
    Use GDAL module to generate Slope raster.
    '''
    #input_Raster = gdal.Open(input_file)
    #OutRaster = input_file.split(".")[0]+"_slp.tif"
    OutRaster = oph.join(OutputDir,oph.basename(input_file).split(".")[0]+"_shd.tif")
    print("Processing : {}".format(OutRaster))
    gdal.DEMProcessing(OutRaster,input_file,"hillshade")   

def SequenceMakeHillshade(input_file):
    for i in input_file:
        MakeHillshade(i)
    

def PrintHelp():
    print("Usage : python DEMProcessing.py (1)Directory (2)Mode (3)Threats")
    print("(1)Directory : Give the directory and the script will process all the rasters.")
    print("(2)Mode : 'hillshade' , 'slope'")
    print("(3)Threads : defaults = 2")
    sys.exit()
    
#======== Muitiprocessing Functions ==========
def multi_task(iter_file,processes=2):
    global Mode
    if Mode == "slope":
        with mps.Pool(processes) as p: #Creating pools
            p.map(MakeSlope,iter_file)
        return
    elif Mode == "hillshade":
        with mps.Pool(processes) as p: #Creating pools
            p.map(MakeHillshade,iter_file)
        return
#======== Argument Parser  ==========
    
def Parser():
    global Threats
    global Mode
    global Directory
    global OutputDir
    argv=sys.argv
    argc=len(argv)
    # --- Parse ---
    if argc == 1 : # No other argument
       PrintHelp()
    elif argc < 3:
        print("Insufficient arguments")
        PrintHelp()
        
    for i in range(1,argc):
        if i == 1:
            if not oph.isdir(argv[i]) :
                print("First argument should be a directory.\n")
                PrintHelp()
            Directory = oph.abspath(argv[i])
            #Change working directory where raster files exists.
            os.chdir(Directory) 
            OutputDir="DEM_Processing_"+time.strftime("%Y%m%d_%H%M%S",time.localtime())
            os.mkdir("DEM_Processing_"+time.strftime("%Y%m%d_%H%M%S",time.localtime()))
            OutputDir=oph.abspath(OutputDir)
        elif i == 2:
            Mode = argv[i].lower()            
            if argv[i].lower() not in ['hillshade','slope']:
                print("Mode should be either 'hillshade' or 'slope'")
                PrintHelp()
        elif i == 3 :
            if int(argv[i]) > mps.cpu_count() or int(argv[i]) == 0:
                print("Please give correct threads. Your PC has {} threads.".format(mps.cpu_count()))
                sys.exit()
            Threats = int(argv[i])
        else : # For future use.
            print("Too many arguments !")
            PrintHelp()
    FileList = dirloader(Directory,"tif")
    
    multi_task(FileList,Threats)

if __name__ == "__main__":
    t=Timer()
    t.start()
    Parser()
    print("Execution time : {} s.".format(t.exec_time()))

"""    
Log:
20161201-2200: V0.1
first version. 
#Functions:
## dirloader(dirpath,extension="*") ：讀取資料夾所有檔案
## MakeSlope(input_file) : 呼叫 GDAL.DEMProcessing 製作 Slope 檔
## PrintHelp() : 印出使用說明
## SequenceMakeSlope(input_file) : 使用序列的方式進行轉檔
## multi_task(iter_file,processes=2) : 使用平行處理模組分配工作
"""
