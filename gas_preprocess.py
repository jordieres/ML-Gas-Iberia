import ast, json, random, argparse
import os, sys, datetime, shutil, pdb
import statistics, smtplib, ssl, re, string
import datetime, math, pickle
import pdb
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.offline as py
import plotly.express as px
import seaborn as sns
#
from plotly import tools
from pathlib import Path
from datetime import date, timedelta
from tempfile import mkstemp
from argparse import ArgumentParser
from urllib.error import URLError
#
#
#
#
# ====================================================================
#
class VAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, const=None, 
                 default=None, type=None, choices=None, required=False, 
                 help=None, metavar=None):
        super(VAction, self).__init__(option_strings, dest, nargs, const, 
                                      default, type, choices, required, 
                                      help, metavar)
        self.values = 0
    def __call__(self, parser, args, values, option_string=None):
        # print('values: {v!r}'.format(v=values))
        if values is None:
            self.values += 1
        else:
            try:
                self.values = int(values)
            except ValueError:
                self.values = values.count('v')+1
        setattr(args, self.dest, self.values)
#
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-w", "--vars", type=str, required=True,
        help="Filename with excel variable structure to be processed")
    ap.add_argument("-f", "--file", type=str, required=True,
        help="Filename with excel dataset to be processed")
    ap.add_argument('-v', nargs='?', action=VAction, dest='verbose')
    #
    args    = vars(ap.parse_args())
    print('{} --> {}'.format(sys.argv[1:], args))
    pfich1  = args["file"]
    pfich2  = args["vars"]
    verbose = args["verbose"]
    if verbose is None:
        verbose = 0
    dat     = carga(pfich1,pfich2,verbose)
    datl    = limpia(dat, verbose)

    return(None)
#
def carga(datf,varf,vrb):
    sheet_to_df_map = pd.read_excel(datf, sheet_name=None)
    sheet_to_df_map.keys()
    odat    = sheet_to_df_map['Sheet1']
    #
    sheet_to_df_map = pd.read_excel(varf, sheet_name=None)
    sheet_to_df_map.keys()
    varindx = sheet_to_df_map['server_taglist_cpi']
    #
    lgrps   = varindx['GLOBAL_CODE_ID'].unique()
    #
    # Quitamos valores no numéricos de los datos
    #
    for k in odat.columns:
        idx = odat[odat[k].apply(lambda x: isinstance(x,str))].index
        if len(idx) > 0:
            if vrb > 1:
                print('Var: '+k + ' => ' + str(len(idx)))
            odat = odat.drop(index=idx)
            odat[k] = odat[k].astype(float)
        else:
            if k != 'index':
                odat[k] = odat[k].astype(float)
    return({'datos':odat,'vindices':varindx,'grupos':lgrps})
#
def limpia(dat,vrb):
    # Se quitan los datos no numéricos y las variables en conflicto
    #
    odat    = dat['datos'].copy()
    varindx = dat['vindices'].copy()
    lgrps   = dat['grupos'].copy()
    # 
    # Corrección de variables: GRLL-MEDAS-EMI-EMISARIO_HR_AMB_UA
    odat = odat.rename(columns={'GRLL0222FQLM1_x': 'GRLL0222FQLM1',
                                'GRLL0220cv1_pos_x':'GRLL0220cv1_pos',
                                'GRLL0220acv1_pos_x': 'GRLL0220acv1_pos'})
    #
    # En varindex hay entradas _x y _y que no sabemos qué hacen pero que no encajan con los datos
    # Variables de datos que no están en la tabla de Jerarquía (después de quitar los _x e _y
    for j in range(1,len(odat.columns)):
        txtj = odat.columns[j]
        idx  = (varindx['TAG'] == txtj)
        ldx  = len(varindx['TAG'].index[idx])
        if ldx == 0:
            if vrb > 0:
                print('*** ERROR: Var no encontrada {}'.format(txtj))
    # Variables de la tabla de Jerarquía que no están en los datos.
    lkeys = '|'.join(odat.columns[1:])
    result = varindx.loc[~varindx['TAG'].str.contains(lkeys, case=False)]
    if (result.shape[0] > 0) & (vrb > 0):    
        print("*** ERROR: Variables de Jerarquía NO USADAS ***")
        print(result['TAG'])
    return({'datos':odat,'vindices':varindx,'grupos':lgrps})
#
if __name__ == "__main__":
    main()
