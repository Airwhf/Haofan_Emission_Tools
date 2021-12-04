#! /usr/bin/env python
#coding=utf-8
#####chemical speciate allocation#####
from src.core.custom_parser import CustomParser
import pandas as pd
config_file_path='create_smoke_to_cmaq.ini'
config = CustomParser(config_file_path)
speciate=pd.read_csv(config['speciate']['speciate'].split(',')[0])
pollutant=speciate['pollutant']
species=speciate['species']
def load_gspro():
    units = {}
    for line in range(len(pollutant)):
        # parse line
        group = pollutant[line].upper()
        speciest = species[line].upper()
        if group == 'PM25' or group=='PMC':
            units[speciest] = 'g/s'
        else:
            units[speciest] = 'moles/s'
    return units