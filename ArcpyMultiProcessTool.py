# -*- coding: utf-8 -*-
_Release = "Arcpy MultiProcessing Release V0.3"
_ReleaseDate = "2016/12/03"
_Authur = "Cheng-Wei Sun"
if __name__ == "__main__":
    print("\n{}\n{} by {}\n".format(_Release,_ReleaseDate,_Authur))
"""
last edited 2016-12-03 1500 @ Sun Home
@author: Sun, Cheng-Wei

Tested platform:
## Windows 10 64 bit
## ArcGIS 10.3 with Python 2.7.10 32 bit

Version Log:
20161201-2200: V0.1
first version.
#Usage : python DEMProcessing.py (1)Directory (2)Mode (3)Extentions (4)Threats
##(1)Directory : Give the directory and the script will process all the rasters.
##(2)Mode : 'hillshade' , 'slope'
##"(3)Extentions : give the extention of the raster. Default is 'tif'
##"(4)Threads : defaults = 2

#Functions:
## dirloader(dirpath,extension="*") ：讀取資料夾所有檔案
## MakeSlope(input_file) : 呼叫 GDAL.DEMProcessing 製作 Slope 檔
## PrintHelp() : 印出使用說明
## SequenceMakeSlope(input_file) : 使用序列的方式進行轉檔
## multi_task(iter_file,processes=2) : 使用平行處理模組分配工作

20161202-2307 v0.2
#Replace multiprocessing.Pool() by multiprocessing.Process() to avoid the initializing problem of arcpy. 
#Remove all content and function related to  pyGDAL.
#Make the stdout more clean (only main process will display importing information).

20161202-1500 v0.3
#Add Hillshade mode
#Change Parser to become more flexible :  (1)Directory (2)Mode maintain fixed.
## Use -t=2 to specify the number of threads
## Use -e=ext to  specify the file extention, default : tif
"""

#===== Built-in libraries =====
from glob import  glob
import os
import os.path as oph
import sys
#from multiprocessing import Pool
import multiprocessing as mps
import time

#===== Initialize Arcpy =====
try:
    if __name__ == "__main__":
        Ts = time.time()
        print("Importing arcpy ...")
    import arcpy
    from arcpy import env
    if __name__ == "__main__":
        print("Done! Time : {}s".format(time.time()-Ts))
except ImportError:
    print("Make sure you have installed \
    ArcGIS and use the python includes the package.")
    sys.exit(1)
if __name__ == "__main__":
    print("Check 3D analyst toolbox and license.")
    
try:
    arcpy.CheckExtension("3D")
    AnalystToolbox_Licence = arcpy.CheckOutExtension("3D")
    if AnalystToolbox_Licence != "CheckedOut":
        print("Fail to initialize 3D analyst toolbox.")
        print("arcpy.CheckOutExtension returns : {}".format(AnalystToolbox_Licence))
        sys.exit()
except:
    print("Error occured when checkout licence of 3D Analyst Toolbox.")
    sys.exit()
#======== Help  ==========
def PrintHelp():
    print("Usage : python DEMProcessing.py (1)Directory (2)Mode (3)-e=Extentions (4)-t=Threats")
    print("(1)Directory : Give the directory and the script will process all the rasters.")
    print("(2)Mode : 'hillshade' , 'slope'")
    print("(3)-e=Extentions : give the extention of the raster. Default is 'tif'")
    print("(4)-t=Threads : defaults = 2")
    sys.exit()
#===== Global Variable =====
Threats = 2
Mode = ""
Directory = ""
OutputDir = None
Ext = "tif"
env.workspace = os.getcwd()
FileList=[]
q = mps.JoinableQueue()
#======== File Load Functions ==========
def dirloader(dirpath,extension="*"):
    '''
    Purpose : spcify a directory then return a list  with all files.
    input : a directory (string), file extention (string)
    output : a list of file matches the condition
    libraries used : os.path (built-in) , glob (built-in)
    '''
    global q
    if sys.platform == "win32" and not dirpath.endswith("\\") :
        pass        
        #dirpath =  dirpath+"\\" 
    dire=oph.abspath(dirpath)
    print("glob path : {}".format(oph.join(dire,"*."+extension)))
    FileList=glob(oph.join(dire,"*."+extension))
    for f in FileList:
        q.put(f)
    return FileList
#======== Argument Parser  ==========
    
def Parser():
    global Threats
    global Mode
    global Directory
    global OutputDir
    global Ext
    global FileList
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
            env.workspace = Directory
            #DEBUG
            #print("OutputDir : {}".format(OutputDir))
            #print("Directory : {}".format(Directory))
            #print("os.getcwd() : {}".format(os.getcwd()))
            #!DEBUG
        elif i == 2:
            Mode = argv[i].lower()            
            if argv[i].lower() not in ['hillshade','slope']:
                print("Mode should be either 'hillshade' or 'slope'")
                PrintHelp()
            #DEBUG            
            #print("Mode : {}".format(Mode))
            #!DEBUG
        elif i == 3 : #Ext 
            if argv[i].startswith("."):
                Ext = argv[i].split(".")[1]
            else :
                 Ext = argv[i]
        elif i == 4 :
            if int(argv[i]) > mps.cpu_count() or int(argv[i]) == 0:
                print("Please give correct threads. Your PC has {} threads.".format(mps.cpu_count()))
                sys.exit()
            Threats = int(argv[i])

        else : # For future use.
            print("Too many arguments !")
            PrintHelp()
    #DEBUG
    #print("CPU count : {} ; Threats setting : {}".format(mps.cpu_count(),Threats))
    #!DEBUG  
    FileList = dirloader(Directory,Ext)
    #DEBUG
    #print(FileList)
    #!DEBUG
    
    #multi_task_py2(FileList,Threats)
    return 

def Parser2():
    global Threats
    global Mode
    global Directory
    global OutputDir
    global Ext
    global FileList
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
            env.workspace = Directory
            #DEBUG
            #print("OutputDir : {}".format(OutputDir))
            #print("Directory : {}".format(Directory))
            #print("os.getcwd() : {}".format(os.getcwd()))
            #!DEBUG
        elif i == 2:
            Mode = argv[i].lower()            
            if argv[i].lower() not in ['hillshade','slope']:
                print("Mode should be either 'hillshade' or 'slope'")
                PrintHelp()
            #DEBUG            
            #print("Mode : {}".format(Mode))
            #!DEBUG
        if   argv[i].startswith("-") : #Valid argument
            arg = argv[i].split("=")
            #----- -t : Threads -----
            if arg[0].lower() == "-t":
                if int(arg[1]) > mps.cpu_count() or int(arg[1]) == 0:
                    print("Please give correct threads. Your PC has {} threads.".format(mps.cpu_count()))
                    sys.exit()      
                Threats = int(arg[1])
            #----- -e : file extension -----
            elif arg[0].lower() == "-e":
                # Check if extension was supported by ArcGIS.
                #https://pro.arcgis.com/en/pro-app/help/data/imagery/supported-raster-dataset-file-formats.htm
                if arg[1] in ["img","asc","bil","bip","bsq","hdr","clr","stx","bmp","bpw","raw","dat","bsq"\
                ,"ige","igw","stk","gif","gfw","jpg","jpeg","jpc","jpe","jgw","jp2", "j2c","j2k","jpx","png","tiff","tif","tff","twf"]:
                    Ext = arg[1]
                else:
                    print("The file format '{}' is not supported.\nSupported file extension is {}".format(\
                        arg[1],'''"img","tiff","tif","tff","twf","asc","bil","bip","bsq","hdr","clr","stx","bmp","bpw","raw","dat","bsq","ige","igw","stk","gif","gfw","jpg","jpeg","jpc","jpe","jgw","jp2","j2c","j2k","jpx","png" ''' ) )
                    sys.exit()
            #----- else : raise error -----
            else:
                print("Unreconized argument {}".format(argv[i]))
                sys.exit()
    FileList = dirloader(Directory,Ext)
    print("Found raster files : {}".format(len(FileList)))
    return

#===== Make slope function  =====
def ArcPyMakeSlope(q,OutputDir,Ext):
    '''Syntax : Slope_3d (in_raster, out_raster, {output_measurement}, {z_factor})    
    '''
    name = mps.current_process().name
    pid=mps.current_process().pid
    print("**" + name + ' Starting **')
    while(True):
        input_file=q.get()
        if  input_file == "*done*":
            print("**" + name + ' Exiting **')
            break
        #arcpy.CheckExtension("3D")
        print("Porcessing raster : {} @ {} ,pid = {}".format(oph.basename(input_file),name,pid) )
        OutRaster = oph.join(OutputDir,oph.basename(input_file).split(".")[0]+"_slp.{}".format(Ext))
        arcpy.Slope_3d(input_file,OutRaster)
        q.task_done()
    q.task_done()
#===== Make Hillshade function  =====
def ArcPyMakeHillshade(q,OutputDir,Ext):
    name = mps.current_process().name
    pid=mps.current_process().pid
    print("**" + name + ' Starting **')
    while(True):
        input_file=q.get()
        if  input_file == "*done*":
            print("**" + name + ' Exiting **')
            break
        #arcpy.CheckExtension("3D")
        print("Porcessing raster : {} @ {} ,pid = {}".format(oph.basename(input_file),name,pid) )
        OutRaster = oph.join(OutputDir,oph.basename(input_file).split(".")[0]+"_shd.{}".format(Ext))
        arcpy.HillShade_3d(input_file,OutRaster)
        q.task_done()
    q.task_done()
if __name__ == "__main__":
    Parser2()
    '''print("Threats {},\n\
Mode {} ,\n\
Directory {},\n\
OutputDir {} ,\n\
Ext {} ,\n\
env.workspace {} ,\n\
FileList {}\n".format(Threats,Mode,Directory,OutputDir,Ext,env.workspace,FileList))'''
    for i in range(Threats):
        q.put("*done*")
    if Mode == "slope":
        for i in range(Threats):
            p = mps.Process(name="Process {}".format(i),target=ArcPyMakeSlope,args=(q,OutputDir,Ext))
            p.start()
    if Mode == "hillshade":
        for i in range(Threats):
            p = mps.Process(name="Process {}".format(i),target=ArcPyMakeHillshade,args=(q,OutputDir,Ext))
            p.start()
    for i in range(Threats):
        p.join()
    print("Check if all files in the queue are processed... If not, kill the process by 'ctrl+c'")
    q.join()
    print("All Done! Time : {} s".format(time.time()-Ts))
