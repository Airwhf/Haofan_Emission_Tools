import os
import geopandas as gpd
import numpy as np
import xarray as xr
from configparser import ConfigParser
import pandas as pd
import subprocess
import netCDF4 as nc
import shutil
import threading
import threadpool


def build_source(ADDL, year, mouthList, totalOutputDir, MEIC_GRID, MEIC_DIR, factorDirPath, toolkitPath):
    # ----------------------------------------------------------------------------------------
    # ADDL: 标识符
    # year: 年份设置 用于获取MEIC文件名
    # mouthList：用于设置需要处理的MEIC清单的月份
    # mouthList = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    # totalOutputDir：输出文件目录
    # MEIC_GRID：网格信息
    #   由“NC转网格”功能得到的shp文件路径

    # MEIC_DIR：输入数据目录
    #   MEIC清单存放目录
    #   注意MEIC的命名格式必须为：
    #   YEAR_MOUTH_DEVELOPMENT_POLLUTION.nc
    #   2019_01_transportation_PM25.nc
    #   关于PM25的设置可以为：
    #   2019_01_transportation_PM2.5.nc 或 2019_01_transportation_PM25.nc
    # factorDirPath：从ISAT中的“多区域*”工具计算得到的Factor文件目录

    # 数据标识设置
    # 程序将通过developmentList中的字符串来寻找MEIC的nc格式文件
    # 但是程序将采用nameLabelList的形式来对不同部门的清单进行命名
    developmentList = ['agriculture', 'industry', 'power', 'residential', 'transportation']
    # 注意这个地方的nameLabelList指在各种目录下的命名寻找 主要是物种分配系数和时间分配系数
    # 例如：当nameLabel为AG的时候
    # 在factorDirPath下的关于AG的排放因子命名应该为AG.csv
    # 在speciateDirPath下的关于AG的物种分配系数应该为speciate_AG.csv
    nameLabelList = ['AG', 'IN', 'PP', 'AR', 'TR']
    # toolkitPath：ISAT目录信息
    #   ISAT的toolkit目录
    # ----------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------
    # 定义输出文件存放路径
    sourceOutDir = f'{totalOutputDir}/source'  # source文件存放路径
    matchOutDir = f'{totalOutputDir}/match'  # 污染物核算输出文件存放路径
    # os.makedirs(sourceOutDir)
    # os.makedirs(matchOutDir)
    # os.makedirs(finalOutDir)
    # ----------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------
    # 此处为Source文件构建
    # 具体流程如下：
    # ----------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------
    # 输入文件： 1. MEIC_GRID：此输入文件通过ISAT中的“NC转网格”功能获取
    #          2. MEIC清单  ：直接从官网获取即可
    #             注意检查   ：MEIC_GRID中的“ID”和“NAME”字段需要检查是否对应
    #                        MEIC清单的文件命名规则为：{year}_{mouth}_{development}_{pollution}.nc
    # ----------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------
    outputDir = sourceOutDir
    # 查看设定的输出目录是否存在，如果不存在则创建。
    if os.path.isdir(outputDir)  == False:
        try:
            os.makedirs(outputDir)
        except:
            print(f'Source输出目录已存在！路径为:{outputDir} \n')

    # NAME CO NH3 NOX PM25 PM10 SO2 VOC
    # 按照“ISAT.toolkit”中”matcharea.exe“输入文件的邀请 定义DataFrame
    data = pd.DataFrame(columns=['NAME', 'CO', 'NH3', 'NOX', 'PM25', 'PM10', 'SO2', 'VOC'], index=None)
    # 读取MEIC清单的网格信息
    # 此输入MEIC_GRID的shapefile文件
    # 该文件可以从ISAT中的“NC转网格”功能实现
    # 但是需要注意的是：该功能中的字段“ID”和“NAME”需要与MEIC清单中nc格式的文件中z变量的索引对应
    # 比如：通过“NC转网格”功能以后，会得到整个MEIC清单中的部分网格
    # 此网格中的“ID”和“NAME”分别是：2， 3， 4， 5
    # 但实际上NC文件中每个网格的对应关系是：0，1， 2， 3
    # 此时将对“ID”和“NAME”字段进行修改，修改的方法有很多，如ArcGIS
    shp_data = gpd.read_file(MEIC_GRID)
    data['NAME'] = shp_data['NAME'].values

    for mouth in mouthList:  # 该循环为逐月对清单进行处理
        # 在该循环中由于涉及到对MEIC文件的读取
        # 因此固定了文件名称的规则：
        # {year}_{mouth}_{development}_{pollution}.nc
        pre_name = f'{year}_{mouth}_'
        for development, nameLabel in zip(developmentList, nameLabelList):
            # pollution_list 初始化文件名中的{pollution}
            # 但不同年份的清单的污染物命名可能会有所不同
            # 因此在此处仅为初始名称列表
            # 此外 pollution_list 变量也是下一步进行污染物核算
            # 即ISAT.toolkit对输入文件表头的要求
            pollution_list = ['CO', 'NH3', 'NOX', 'PM25', 'PM10', 'SO2', 'VOC']
            for pollution in pollution_list:
                result = []
                # 由于 PM10 的数值可能无法从MEIC某些年份的清单中直接得到
                # 因此MEIC清单中没有提供PM10的污染物
                # 所以 我们采用PM2.5+PMcoarse 来定义PM10
                # 而其他污染物均能直接从MEIC中直接获取
                if pollution == 'PM10':
                    try:
                        file_1 = r'%s/%s%s_%s.nc' % (MEIC_DIR, pre_name, development, 'PM10')
                        temp_data = xr.open_dataset(file_1)['z'].values
                        for i in shp_data['NAME'].values:
                            temp_result = temp_data[int(i)]
                            result.append(temp_result)
                        data['PM10'] = np.array(result)
                    except:
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
            # 至此，完成对source文件的构建。
    # ----------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------
    # 调用matcharea.exe核算污染物排放量
    # ----------------------------------------------------------------------------------------
    # 定义从MEIC清单中提取出来的Source文件目录
    # 此处直接输入上一步骤中的sourceOutDir即可
    sourceDirPath = sourceOutDir 
    # Source文件和Factor文件nameList此变量进行命名
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
            print(f'污染物核算输出目录已存在！路径为:{outputDirPath} \n')

    for mouth in mouthList:
        for name, nameLabel in zip(nameList, nameLabelList):
            cfg = ConfigParser()
            cfg.read(configFile)
            # cfg.sections()
            cfg.set('matcharea', 'source', f'{sourceDirPath}/source_{mouth}_{nameLabel}.csv')
            cfg.set('matcharea', 'factor', f'{factorDirPath}/{nameLabel}.csv')
            cfg.set('outfile', 'matcharea', f'{outputDirPath}/{mouth}_{nameLabel}.csv')
            cfg.write(open(configFile, 'w'))

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
            result['co'] = data['CO']
            result['pm25'] = data['PM25']
            result['pm10'] = data['PM10']
            result['nh3'] = data['NH3']
            # print(result)
            result.to_csv(f'{outputDirPath}/{mouth}_{nameLabel}.csv', index=False)
            # print(f'完成了对{name}的修正')

            # 删除临时脚本文件
            os.remove(scriptName)


def build_emission(gridPath, date, sourceDirPath, speciateDirPath, temporaryDirPath,
                   outputDirPath, CTRL_MERGE = True):
    # gridPath: CRIDCRO2D文件路径
    # date: 处理日期 格式：'YYYY-mm-dd' 字符串
    # sourceDirPath：源网格排放文件存放路径
    # nameLabelList：输出文件和输入文件标识符
    nameLabelList = ['AG', 'IN', 'PP', 'AR', 'TR']
    # speciateDirPath：物种分配谱文件路径
    #   将名称设置为 speciate_{nameLabel} 格式
    #   {nameLabel}为 nameLabelList 中的元素
    # temporaryDirPath: 时间谱分配文件目录
    #   时  分配谱名称为 hourly.csv
    #   周  分配谱名称为 weekly.csv
    #   月  分配谱名称为 mouthly.csv
    # outputDirPath：输出文件目录
    # CTRL_MERGE：是否合并分部门排放 默认为：True

    date_grid = date  # 此变量用于设置到GRIDCRO2D.nc文件中
    date_label = f'{date_grid[0:4]}{date_grid[5:7]}{date_grid[8:10]}'  # 此变量用于设置到输出文件中
    start_year = f'{date_grid[0:4]}'  # 此变量用于修改GRIDCRO2D中的日期
    mouth = f'{date_grid[5:7]}'

    # 创建输出目录
    try:
        os.mkdir(outputDirPath)
    except:
        print(f'工作空间已经存在！路径为：{outputDirPath}')

    # 创建一个新的ISAT.M工作空间
    ISAT_M_Path = f'{os.getcwd()}/ISAT.M_{date_label}'
    ISAT_M_Path = ISAT_M_Path.replace('\\', '/')
    # print(ISAT_M_Path)
    try:
        shutil.copytree('ISAT.M', ISAT_M_Path)
    except:
        print(f'工作空间已经存在！路径为：{ISAT_M_Path}')

    # 修改GRIDCRO2D文件中的起始日期
    start_date = pd.to_datetime(f'{start_year}-01-01')
    end_date = pd.to_datetime(date_grid)
    delta = end_date - start_date
    delta_day = r'%.3d' % (int(delta / pd.Timedelta(1, 'D')) + 1)

    # 复制grid_path文件
    shutil.copy(gridPath, f'{gridPath}_{start_year}{delta_day}')
    gridPath = f'{gridPath}_{start_year}{delta_day}'
    gridData = nc.Dataset(gridPath, 'a')
    gridData.setncattr('SDATE', int(f'{start_year}{delta_day}'))
    gridData.close()

    for nameLabel in nameLabelList:

        # 变更原始文件路径
        emissionPath = f'{sourceDirPath}/{mouth}_{nameLabel}.csv'
        # 创建文件目录
        if os.path.isdir(f'{sourceDirPath}/{date_label}') == False:
            try:
                os.makedirs(f'{sourceDirPath}/{date_label}')
            except:
                print(f'目录已存在！路径名为: {sourceDirPath}/{date_label}')

        shutil.copy(emissionPath, f'{sourceDirPath}/{date_label}/{nameLabel}.csv')
        emissionPath = f'{sourceDirPath}/{date_label}/{nameLabel}.csv'

        # 构建减排文件
        results = pd.DataFrame(columns=[f'{nameLabel}'], index=None)
        data = pd.read_csv(emissionPath)
        results[f'{nameLabel}'] = np.zeros(data.shape[0]) + 1
        results.to_csv(f'{ISAT_M_Path}/src/control/areacontrol.csv', index=False)

        # 设置配置文件
        configFile = f'{ISAT_M_Path}/create_smoke_to_cmaq.ini'
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
        cfg.set('control', 'areacontrol', f'{ISAT_M_Path}/src/control/areacontrol.csv')
        cfg.write(open(configFile, 'w'))

        # 创建批处理脚本
        scriptName = f'script_area_inlinenew_{date_label}_{nameLabel}.bat'
        f = open(scriptName, 'w')
        f.write(f'{ISAT_M_Path[0:2]} \n')
        f.write(f'cd {ISAT_M_Path} \n')
        f.write('area_inlinenew.exe \n')
        f.close()

        # 执行bat脚本
        p = subprocess.Popen(scriptName, shell=True, stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            print(f'执行 area_inlinenew.exe 失败：{date_label}的{nameLabel}')  # is 0 if success
        else:
            # 修改输出文件名称
            shutil.copy(f'{ISAT_M_Path}/{nameLabel}area.nc', f'{outputDirPath}/{date_label}_{nameLabel}area.nc')

        # 删除临时脚本文件
        os.remove(scriptName)
        # 删除临时gridpath
        os.remove(gridPath)

    # 删除工作空间
    shutil.rmtree(ISAT_M_Path)

    # 合并文件
    if CTRL_MERGE:
        input_dir = outputDirPath
        file_list = []
        for file_name in os.listdir(input_dir):
            if date_label == file_name[0:8]:
                file_list.append(f'{input_dir}/{file_name}')

        if len(file_list) != 0:
            outputName = f'{input_dir}/emission_{date_label}.nc'
            shutil.copy(file_list[0], outputName)
            model_data = nc.Dataset(file_list[0])
            var_list = model_data.getncattr('VAR-LIST').split()

            dset = nc.Dataset(outputName, 'a')
            for var in var_list:
                value = np.zeros(model_data[var_list[0]][:].shape)
                for file_name in file_list:
                    if file_name == outputName:
                        continue
                    data = nc.Dataset(file_name)
                    temp_value = data[var][:]
                    data.close()
                    temp_value[pd.isna(temp_value)] = 0.0
                    temp_value[np.where(temp_value > 1.0)] = 0.0
                    value = value + temp_value
                dset[var][:] = value
            dset.close()
    print(f'清单制作成功！日期：{date}')


if __name__ == '__main__':

    # 全局变量
    # 设置处理清单的起始时间
    startDate = '2021-12-01'
    # 设置处理清单的结束时间
    endDate = '2021-12-31'

    CTRL_SOURCE = True
    # 如果需要构建Source文件----------------------------------------------------------
    # 设置标识符
    ADDL = '2021'
    # 设置使用的清单年份
    emission_year = f'2017'
    # 设置输出路径
    output_Dir = f'E:/Chengdu_emis/{ADDL}'
    # 设置MEIC矢量网格路径
    MEIC_GRID = 'G:/domain/Chengdu-3km/NC转网格/2017_01_agriculture_NH3—矢量化.shp'
    # 设置MEIC清单目录
    MEIC_DIR = f'E:/MEIC/2017'
    # 设置分配因子文件
    factorDirPath = 'E:/成都网格/factor'
    # 设置ISAT.toolkit路径(绝对路径)
    toolkitPath = 'G:/python_project/Haofan_Emission_Tools/ISAT.toolkit'

    # 如果不需要构建Source文件---------------------------------------------------------
    # GRIDCRO2D文件路径
    gridPath = 'D:/Model/build_cmaq/CMAQ-master/data/chengdu_pa/mcip/GRIDCRO2D_211203.nc'
    # 物种谱分配文件目录
    #   将名称设置为 speciate_{nameLabel} 格式
    speciateDirPath = 'E:/ISAT/清单制作常用文件/物种分配系数案例'
    # 时间谱分配文件目录
    #   时  分配谱名称为 hourly.csv
    #   周  分配谱名称为 weekly.csv
    #   月  分配谱名称为 mouthly.csv
    temporaryDirPath = 'E:/ISAT/清单制作常用文件/时间分配系数案例'
    # 源网格排放文件存放路径
    sourceDirPath = f'E:/Chengdu_emis/{ADDL}/match'
    # 输出文件路径
    outputDirPath = f'E:/Chengdu_emis/{ADDL}/CMAQ'
    # 并行CPU数量
    thread_number = 24

    try:
        date_list = pd.date_range(startDate, endDate)
        date_grid_list = date_list.strftime('%Y-%m-%d')
        date_label_list = date_list.strftime('%Y%m%d')
        start_year_list = date_list.strftime('%Y')
        mouth_list = date_list.strftime('%m')
    except:
        print(f"请设置正确的日期！")

    # 通过输入起始时间获取mouthList
    mouthList = np.unique(np.array(mouth_list))

    if CTRL_SOURCE:
        # 构建source文件
        build_source(ADDL, emission_year, mouthList, output_Dir, MEIC_GRID, MEIC_DIR, factorDirPath, toolkitPath)

    pool = threadpool.ThreadPool(thread_number)
    # 构建参数表
    func_var = []
    for date in date_grid_list:
        lst_vars = ([gridPath, date, sourceDirPath, speciateDirPath, temporaryDirPath, outputDirPath], None)
        func_var.append(lst_vars)

    # 启动线程池
    requests = threadpool.makeRequests(build_emission, func_var)
    [pool.putRequest(req) for req in requests]
    pool.wait()

