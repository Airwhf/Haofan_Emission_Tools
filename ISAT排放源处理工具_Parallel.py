import os
import sys
import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr
from configparser import ConfigParser
import pandas as pd
import subprocess
import netCDF4 as nc
import shutil

def run(ADDL, mouthList):
    # cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

    ### 基本信息 ###
    # 标识符
    # ADDL = '2014'
    # 年份设置
    year = f'{ADDL}'
    # 设置排放清单年份
    ## 如果设置为None 则自动获取GRIDCRO2D文件中的年份作为清单年份
    syear = f'{ADDL}'
    # 月份设置
    # mouthList = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    ### 输出文件目录 ###
    totalOutputDir = f'H:/emission/{ADDL}'

    ### 网格信息 ###
    # 由“NC转网格”功能得到的shp文件路径
    MEIC_GRID = 'E:/Kai Wu/NC转网格.shp'
    # GRIDCRO2D文件路径
    gridPath = 'E:/Kai Wu/mcip/GRIDCRO2D_2019094.nc'

    ### 输入数据目录 ###
    # MEIC清单存放目录
    # 注意MEIC的命名格式必须为：
    # YEAR_MOUTH_DEVELOPMENT_POLLUTION.nc
    # 2019_01_transportation_PM25.nc
    # 关于PM25的设置可以为：
    # 2019_01_transportation_PM2.5.nc 或 2019_01_transportation_PM25.nc
    MEIC_DIR = f'E:/MEIC/{ADDL}'
    # 从ISAT中的“多区域*”工具计算得到的Factor文件目录
    factorDirPath = 'E:/Kai Wu/factor' 
    # 物种谱分配文件目录
    # 将名称设置为 speciate_{nameLabel} 格式
    speciateDirPath = 'E:/ISAT/清单制作常用文件/物种分配系数案例'
    # 时间谱分配文件目录
    # 时  分配谱名称为 hourly.csv
    # 周  分配谱名称为 weekly.csv
    # 月  分配谱名称为 mouthly.csv
    temporaryDirPath = 'E:/ISAT/清单制作常用文件/时间分配系数案例'

    ### 数据标识设置 ###
    ## 程序将通过developmentList中的字符串来寻找MEIC的nc格式文件
    ## 但是程序将采用nameLabelList的形式来对不同部门的清单进行命名
    developmentList = ['agriculture', 'industry', 'power', 'residential', 'transportation']
    # 注意这个地方的nameLabelList指在各种目录下的命名寻找 主要是物种分配系数和时间分配系数
    # 例如：当nameLabel为AG的时候
    # 在factorDirPath下的关于AG的排放因子命名应该为AG.csv
    # 在speciateDirPath下的关于AG的物种分配系数应该为speciate_AG.csv
    nameLabelList = ['AG', 'IN', 'PP', 'AR', 'TR']

    ### ISAT目录信息###
    # ISAT的toolkit目录
    toolkitPath = f'E:/ISAT/ISAT.toolkit_{mouthList[0]}'  
    # ISAT.M程序目录
    isatmDirPath = f'E:/ISAT/ISAT.M_{mouthList[0]}'
    # cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

    # ----------------------------------------------------------------------------------------
    sourceOutDir = f'{totalOutputDir}/source'
    matchOutDir = f'{totalOutputDir}/match'
    finalOutDir = f'{totalOutputDir}/CMAQ'
    # os.makedirs(sourceOutDir)
    # os.makedirs(matchOutDir)
    # os.makedirs(finalOutDir)
    # ----------------------------------------------------------------------------------------

    # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    outputDir = sourceOutDir
    # 构建Source文件
    if os.path.isdir(outputDir)  == False:
        try:
            os.makedirs(outputDir)
        except:
            print(f'directory existed. Name:{outputDir}')

    # NAME CO NH3 NOX PM25 PM10 SO2 VOC
    data = pd.DataFrame(columns=['NAME', 'CO', 'NH3', 'NOX', 'PM25', 'PM10', 'SO2', 'VOC'], index=None)
    shp_data = gpd.read_file(MEIC_GRID)
    data['NAME'] = shp_data['NAME'].values

    for mouth in mouthList:
        pre_name = f'{year}_{mouth}_'
        for development, nameLabel in zip(developmentList, nameLabelList):
            pollution_list = ['CO', 'NH3', 'NOX', 'PM25', 'PM10', 'SO2', 'VOC'] # PMcoarse
            for pollution in pollution_list:
                result = []
                if pollution == 'PM10':
                    try:
                        
                        try:
                            file_1 = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM25')
                            temp_data_1 = xr.open_dataset(file_1)['z'].values
                        except:
                            file_1 = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM2.5')
                            temp_data_1 = xr.open_dataset(file_1)['z'].values

                        file_2 = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PMcoarse')        
                        temp_data_2 = xr.open_dataset(file_2)['z'].values
                        for i in shp_data['NAME'].values:
                            temp_result = temp_data_1[int(i)] + temp_data_2[int(i)]
                            result.append(temp_result)
                        data['PM10'] = np.array(result)
                    except:
                        file_1 = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM10')
                        temp_data = xr.open_dataset(file_1)['z'].values
                        for i in shp_data['NAME'].values:
                            temp_result = temp_data[int(i)]
                            result.append(temp_result)
                        data[pollution] = np.array(result)
                else:
                    if pollution == 'PM25':
                        try:
                            file = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM25')
                            temp_data = xr.open_dataset(file)['z'].values
                            for i in shp_data['NAME'].values:
                                temp_result = temp_data[int(i)]
                                result.append(temp_result)
                            data[pollution] = np.array(result)
                        except:
                            file = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM2.5')
                            temp_data = xr.open_dataset(file)['z'].values
                            for i in shp_data['NAME'].values:
                                temp_result = temp_data[int(i)]
                                result.append(temp_result)
                            data[pollution] = np.array(result)
                    else:
                        file = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, pollution)
                        temp_data = xr.open_dataset(file)['z'].values
                        for i in shp_data['NAME'].values:
                            temp_result = temp_data[int(i)]
                            result.append(temp_result)
                        data[pollution] = np.array(result)

            output_name = f'{outputDir}/source_{mouth}_{nameLabel}.csv'
            data.to_csv(output_name, index=False)
            # print(r'output: %s' % output_name)
    # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

    # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    # 调用matcharea.exe核算污染物排放量
    # ----------------------------------------------------------------------------------------
    # 从MEIC清单中提取出来的Source文件目录
    sourceDirPath = sourceOutDir 
    # Source文件和Factor文件以此变量进行命名 
    # 此处默认Factor同一年中不会有改变
    nameList = developmentList  
    configFile = f'{toolkitPath}/create_smoke_to_cmaq.ini'
    # 输出文件目录，具体文件命参考NameList
    # 建议将此目录直接设置到'*/src/area'
    outputDirPath = matchOutDir  
    # ----------------------------------------------------------------------------------------

    if os.path.isdir(outputDirPath)  == False:
        try:
            os.makedirs(outputDirPath)
        except:
            print(f'directory existed. Name:{outputDirPath}')

    for mouth in mouthList:
        for name, nameLabel in zip(nameList, nameLabelList):
            cfg = ConfigParser()
            cfg.read(configFile)
            # cfg.sections()
            cfg.set('matcharea', 'source', f'{sourceDirPath}/source_{mouth}_{nameLabel}.csv')
            cfg.set('matcharea', 'factor', f'{factorDirPath}/{nameLabel}.csv')
            cfg.set('outfile', 'matcharea', f'{outputDirPath}/{mouth}_{nameLabel}.csv')
            cfg.write(open(configFile, 'w'))
            # cfg.get('matcharea', 'source')
            # cfg.get('matcharea', 'factor')
            # cfg.get('outfile', 'matcharea')

            # 创建调用脚本
            scriptName = f'Script_{ADDL}_{mouth}_{nameLabel}.bat'
            f = open(scriptName, 'w')
            f.write(f'{toolkitPath[0:2]} \n')
            f.write(f'cd {toolkitPath} \n')
            f.write('matcharea.exe \n')
            f.close()

            # 执行bat脚本
            p = subprocess.Popen(scriptName, shell=True, stdout = subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != 0:
                print(f'执行 matcharea 失败：{name}')# is 0 if success

            # 修正文件格式
            result = pd.DataFrame(columns=['lon', 'lat', 'so2', 'no2', 'voc', 'co', 'pm25', 'pm10', 'nh3'], index=None)
            data = pd.read_csv(f'{outputDirPath}/{mouth}_{nameLabel}.csv')
            # print(data)
            result['lon'] = data['LON']
            result['lat'] = data['LAT']
            result['so2'] = data['SO2']
            result['no2'] = data['NOX']
            result['voc'] = data['VOC']
            result['co']  = data['CO']
            result['pm25']= data['PM25']
            result['pm10']= data['PM10']
            result['nh3'] = data['NH3']
            # print(result)
            result.to_csv(f'{outputDirPath}/{mouth}_{nameLabel}.csv', index=False)
            # print(f'完成了对{name}的修正')

            # 删除临时脚本文件
            os.remove(scriptName)
    # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

    # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    # 面源排放清单文件目录
    # 文件名设置为 月份_部门 如：01_AR.csv 代表1月分AR部门的排放
    sourceDirPath = matchOutDir

    # 设置输出排放清单月份 所有时次均从当月1日开始
    setStartMouthList = mouthList
    # 设置输出文件目录
    outputDirPath = finalOutDir

    # 创建文件目录
    if os.path.isdir(outputDirPath)  == False:
        try:
            os.makedirs(outputDirPath)
        except:
            print(f'directory existed. Name:{outputDirPath}')

    # 获取起始年份 如果没有进行定义 会从当前GRIDCRO2D文件中获取
    if syear is None:
        gridData = nc.Dataset(gridPath, 'a')
        syear = str(gridData.getncattr('SDATE'))[0:4]  # 获取年份
        gridData.close()

    # 取闰年或平年
    if int(syear) % 4 == 0:
        mouthDays = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] 
    else:
        mouthDays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] 

    for setStartMouth in setStartMouthList:
        for deltaDay in range(mouthDays[int(setStartMouth)-1]):

            gridData = nc.Dataset(gridPath, 'a')
            # syear = str(gridData.getncattr('SDATE'))[0:4]  # 获取年份
            ## 计算当年 当月 日序
            start_date = pd.to_datetime(f'{syear}-01-01')
            end_date = pd.to_datetime(f'{syear}-{setStartMouth}-01')
            delta = end_date - start_date
            delta_day = r'%.3d' % (int(delta/pd.Timedelta(1, 'D')) + 1 + deltaDay)
            # 当前日期
            temp_date = end_date + pd.Timedelta(deltaDay, 'D')
            temp_date = str(temp_date)
            temp_date = f'{temp_date[0:4]}{temp_date[5:7]}{temp_date[8:10]}'

            ## 修改GRIDCRO2D文件起始时间
            gridData.setncattr('SDATE', int(f'{syear}{delta_day}'))
            gridData.close()

            for nameLabel in nameLabelList:

                # 变更原始文件路径
                emissionPath = f'{sourceDirPath}/{setStartMouth}_{nameLabel}.csv'
                # 创建文件目录
                if os.path.isdir(f'{sourceDirPath}/{mouth}')  == False:
                    try:
                        os.makedirs(f'{sourceDirPath}/{mouth}')
                    except:
                        print(f'directory existed. Name: {sourceDirPath}/{mouth}')

                shutil.copy(emissionPath, f'{sourceDirPath}/{mouth}/{nameLabel}.csv')
                emissionPath = f'{sourceDirPath}/{mouth}/{nameLabel}.csv'

                # 构建减排文件
                results = pd.DataFrame(columns=[f'{nameLabel}'], index=None)
                data = pd.read_csv(emissionPath)
                results[f'{nameLabel}'] = np.zeros(data.shape[0])+1
                results.to_csv(f'{isatmDirPath}/src/control/areacontrol.csv', index=False)

                # 设置配置文件
                configFile = f'{isatmDirPath}/create_smoke_to_cmaq.ini'
                cfg = ConfigParser()
                cfg.read(configFile)
                cfg.set('model', 'model', 'CMAQinline')
                cfg.set('inputtype', 'inputtype', 'mouth')  # 更改排放类别 月度排放 or 年度排放
                cfg.set('runtime', 'runtime', '25')
                cfg.set('gridcro2d', 'gridcro2d', gridPath)
                cfg.set('speciate', 'speciate', f'{speciateDirPath}/speciate_{nameLabel}.csv')
                cfg.set('temporary', 'temporary_hour', f'{temporaryDirPath}/hourly.csv')
                cfg.set('temporary', 'temporary_week', f'{temporaryDirPath}/weekly.csv')
                cfg.set('temporary', 'temporary_month', f'{temporaryDirPath}/monthly.csv')
                cfg.set('emissions', 'emissions', emissionPath)
                cfg.set('control', 'areacontrol', f'{isatmDirPath}/src/control/areacontrol.csv')
                cfg.write(open(configFile, 'w'))

                # 创建批处理脚本
                scriptName = f'script_area_inlinenew_{ADDL}_{temp_date}_{nameLabel}.bat'
                f = open(scriptName, 'w')
                f.write(f'{isatmDirPath[0:2]} \n')
                f.write(f'cd {isatmDirPath} \n')
                f.write('area_inlinenew.exe \n')
                f.close()

                # 执行bat脚本
                p = subprocess.Popen(scriptName, shell=True, stdout = subprocess.PIPE)
                stdout, stderr = p.communicate()
                if p.returncode != 0:
                    print(f'执行 area_inlinenew.exe 失败：{temp_date}的{nameLabel}')# is 0 if success
                else:
                    # 修改输出文件名称
                    shutil.copy(f'{isatmDirPath}/{nameLabel}area.nc', f'{outputDirPath}/{temp_date}_{nameLabel}area.nc')

                # 删除临时脚本文件
                os.remove(scriptName)

            # ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc    
            input_dir = finalOutDir

            prefixDateList = [temp_date]
            # for sname in os.listdir(input_dir):
            #     prefixDateList.append(sname[0:8])
                
            # prefixDateList = np.array(prefixDateList)
            # prefixDateList = np.unique(prefixDateList)
            # print(prefixDate)
            for prefixDate in prefixDateList:
                file_list = []
                for file_name in os.listdir(input_dir):
                    if prefixDate in file_name:
                        file_list.append(f'{input_dir}/{file_name}')
                # print(r'File count: %s' % len(file_list))
                if len(file_list) == 0:
                    continue
                else:
                    outputName = f'{input_dir}/emission_{prefixDate}.nc'
                    shutil.copy(file_list[0], outputName)
                    model_data = nc.Dataset(file_list[0])
                    var_list = model_data.getncattr('VAR-LIST').split()
                    
                    dset = nc.Dataset(outputName, 'a')
                    for var in var_list:
                        value = np.zeros(model_data[var_list[0]][:].shape)
                        for file_name in file_list:
                            # 跳过merge.nc文件
                            if file_name == outputName:
                                continue
                            # print('当前处理文件：%s 当前处理变量：%s' % (file_name, var))
                            # print(file_name)
                            data = nc.Dataset(file_name)
                            data = data[var][:]
                            # print(pd.isna(data))
                            data[pd.isna(data)] = 0.0
                            data[np.where(data > 1.0)] = 0.0
                            # print(np.where(data == np.nan))
                            value = value + data

                        dset[var][:] = value
                        # print('Write %s to the merge file.' % var)
                        # print('-----------------------------%s END--------------------------------' % var)
                    dset.close()
                    # print(r'Output: %s' % r'%s/%s' % (input_dir, 'merge.nc'))            

import threading
# threads = []
# t1 = threading.Thread(target=run,args=(ADDL, ))
# threads.append(t1)
# t2 = threading.Thread(target=run,args=('2015',))
# threads.append(t2)
# t3 = threading.Thread(target=run,args=('2016',))
# threads.append(t3)
# t4 = threading.Thread(target=run,args=('2017',))
# threads.append(t4)
# t5 = threading.Thread(target=run,args=('2019',))
# threads.append(t5)
# t6 = threading.Thread(target=run,args=('2020',))
# threads.append(t6)

if __name__ == '__main__':

    # 设置运行年份
    ADDL = '2016'

    # 设置mouthList
    mouthList = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    # 工作空间副本
    ISAT_PARENT = 'E:/ISAT'
    ISATM_ORIN = 'E:/ISAT/ISAT.M'
    ISATT_ORIN = 'E:/ISAT/ISAT.toolkit'


    # 创建独立的工作空间
    for mouth in mouthList:
        if os.path.isdir(f'{ISAT_PARENT}/ISAT.M_{mouth}')  == False:
            try:
                shutil.copytree(ISATM_ORIN, f'{ISAT_PARENT}/ISAT.M_{mouth}')
            except:
                print(f'directory existed. Name: {ISAT_PARENT}/ISAT.M_{mouth}')

        if os.path.isdir(f'{ISAT_PARENT}/ISAT.toolkit_{mouth}')  == False:
            try:
                shutil.copytree(ISATT_ORIN, f'{ISAT_PARENT}/ISAT.toolkit_{mouth}')
            except:
                print(f'directory existed. Name: {ISAT_PARENT}/ISAT.toolkit_{mouth}')

    threads = []

    for mouth in mouthList:
        t1 = threading.Thread(target=run,args=(ADDL, [mouth],))
        threads.append(t1)

    for t in threads:
        # t.setDaemon(True)
        t.start()

