# ISAT排放工具

通过ISAT获取**多区域栅格排放因子**,结合“NC转网格”功能**获取的MEIC网格序号**，提取**MEIC清单数据**，进行**降尺度**。

## 输入参数说明

清单起始时间和结束时间，格式（YYYY-MM-DD）

    startDate = '2021-12-01'

    endDate = '2021-12-31'

程序会按天输出在此阶段的所有清单。

如果需要构建**源排放文件**：

    CTRL_SOURCE = True
如果不需要：

    CTRL_SOURCE = False
源排放文件是运行ISAT.M的必要文件，如果已经构建，则不需要重复。这将会节省一部分时间。

如果需要构建Source文件，则还需要输入以下参数。

ADDL参数不会对程序造成实质性的影响，仅用于帮助用户区别每次运行案例。

    ADDL = '2021'
此参数用于设置使用的MEIC清单年份。

    emission_year = f'2017'

此参数用于设置源排放文件的输出路径。

    output_Dir = f'E:/Chengdu_emis/{ADDL}'
设置MEIC矢量网格路径。

    MEIC_GRID = 'G:/domain/Chengdu-3km/NC转网格/2017_01_agriculture_NH3—矢量化.shp'
设置MEIC清单目录

    MEIC_DIR = f'E:/MEIC/2017'
设置分配因子文件

    factorDirPath = 'E:/成都网格/factor'
设置ISAT.toolkit路径(绝对路径)

此文件可以在百度网盘下载。

链接：https://pan.baidu.com/s/1FPoW6kK9icq_U9ErR5vM7Q 

提取码：hrzb 

    toolkitPath = 'G:/python_project/Haofan_Emission_Tools/ISAT.toolkit'
---------------------------
如果不需要构建Source文件可以仅设置以下参数。

GRIDCRO2D文件路径

    gridPath = 'D:/Model/build_cmaq/CMAQ-master/data/chengdu_pa/mcip/GRIDCRO2D_211203.nc'
物种谱分配文件目录

将名称设置为 speciate_{nameLabel} 格式

nameLabel包括AR AG PP IN TR

    speciateDirPath = 'E:/ISAT/清单制作常用文件/物种分配系数案例'

时间谱分配文件目录

时  分配谱名称为 hourly.csv

周  分配谱名称为 weekly.csv

月  分配谱名称为 mouthly.csv

    temporaryDirPath = 'E:/ISAT/清单制作常用文件/时间分配系数案例'

源网格排放文件存放路径

    sourceDirPath = f'E:/Chengdu_emis/{ADDL}/match'
输出文件路径

    outputDirPath = f'E:/Chengdu_emis/{ADDL}/CMAQ'
并行CPU数量

    thread_number = 24
          