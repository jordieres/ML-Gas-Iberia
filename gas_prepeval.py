#
# Starting from the excel file provided with variable values
# and the file with the description of variables we will prepare a 
# pickle file with records after a date.
#
# The pickle file will be used in eval_models.py
# (C) UPM.  JOM 2023-11-30
# 
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
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid date: {0!r}".format(s)
        raise argparse.ArgumentTypeError(msg)
#
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-w", "--vars", type=str, required=True,
        help="Filename with excel variable structure to be processed")
    ap.add_argument("-f", "--file", type=str, required=True,
        help="Filename with excel dataset to be processed")
    ap.add_argument("-d", "--date", type=valid_date, required=True,
        help="From Date for prepare for assessment. Format YYYY-MM-DD")
    ap.add_argument("-o", "--output", type=str, required=True,
        help="Filename for the pickle container. Row1: dat; Row2: Vars; Row3: Groups; Row4: Dat per group")
    ap.add_argument('-v', nargs='?', action=VAction, dest='verbose')
    #
    args    = vars(ap.parse_args())
    # print('{} --> {}'.format(sys.argv[1:], args))
    pfich1  = args["file"]
    pfich2  = args["vars"]
    pfich3  = args["output"]
    cdate   = args["date"]
    verbose = args["verbose"]
    if verbose is None:
        verbose = 0
    dat     = carga(pfich1,pfich2,verbose)
    datl    = limpia(dat, verbose)
    dats    = segmenta(datl,cdate,verbose)
    #
    with open(pfich3, "wb") as output_file:
        pickle.dump(dats['grupos'],output_file,pickle.HIGHEST_PROTOCOL)
        pickle.dump(dats['datgrps'],output_file,pickle.HIGHEST_PROTOCOL)
    #
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
def var_map(j,nname):
    if j == 'index':
        return('Time:ISO')
    return(nname.loc[nname['TAG']==j,'NNORM'].tolist()[0])
#
def segmenta(dat,cdate,vrb):
    # Segmentación por Sistemas
    #
    odat    = dat['datos'].copy()
    varindx = dat['vindices'].copy()
    lgrps   = dat['grupos'].copy()
    clean   = varindx.loc[varindx['DESCRIPCION'].str.contains(
                        'POTENCIA ACTIVA'),['TAG','GLOBAL_CODE_ID']]
    dtime   = {}
    for i in clean.index:
        varj= clean.loc[i,'TAG']
        grp = clean.loc[i,'GLOBAL_CODE_ID']
        toff= odat.loc[odat[varj] < 15,:].index # Identificando < 15MW
        if len(toff) > 0:
            tmv = odat.index.get_loc(toff[0])
        for j in range(1,len(toff)):
            tmvp= odat.index.get_loc(toff[j])
            tmv = tmv.union(tmvp)
        dtime[grp] = tmv
    dosunouno = odat.loc[list(set(odat.index) - set(
                dtime[71].union(dtime[72]).union(dtime[76])))].index
    dostodo   = odat.loc[list(set(odat.index) - set(
                dtime[71].union(dtime[72])))].index
    unounocero= odat.loc[list(set(dostodo) - set(dosunouno) )].index
    unotodovap= odat.loc[list(set(odat.index) - set(
                dtime[71]).union(dtime[76]))].index
    unocerouno= odat.loc[list(set(unotodovap) - set(dosunouno) )].index
    todounocer= odat.loc[list(set(odat.index) - set(
                dtime[72]).union(dtime[76]))].index
    cerounouno= odat.loc[list(set(todounocer) - set(dosunouno) )].index
    off       = odat.loc[list(set(dtime[71].union(
                dtime[72]).union(dtime[76])) )].index
    odat.loc[off,'LABEL']        = '0+0+0'
    odat.loc[dosunouno,'LABEL']  = '1+1+1'
    odat.loc[unounocero,'LABEL'] = '1+1+0'
    odat.loc[unocerouno,'LABEL'] = '1+0+1'
    odat.loc[cerounouno,'LABEL'] = '0+1+1'
    if vrb > 1:
        print(odat['LABEL'].value_counts())
    #
    datgrp = {}
    lkeys  = '|'.join(odat.columns[1:])
    r      = re.compile("GRLL-MEDAS.*")
    odat   = odat.loc[odat['index'] >= cdate,:]
    for i in lgrps:
        if vrb > 0:
            print(' *** Procesando grupo:' + str(i))
        nname   = varindx.loc[varindx['GLOBAL_CODE_ID'].isin([i]),['TAG',
                                'GLOBAL_CODE_ID','DESCRIPCION','UNIDAD']]
        nlist   = list(filter(r.match, odat.columns))
        for j in nlist:
            if j not in nname['TAG'].tolist():
                nname2  = varindx.loc[varindx['TAG'].isin([j]),['TAG', \
                                'GLOBAL_CODE_ID','DESCRIPCION','UNIDAD']]
                nname   = pd.concat([nname,nname2], axis=0)
        nname   = nname.loc[nname['TAG'].str.contains(lkeys, case=False)]
        nname['NNORM'] = nname['DESCRIPCION'].str.replace(' ','_')+':'+ \
                    nname['UNIDAD'].astype(str)
        # Nombres explicativos de las varaibles creados
        dat_unt = odat.loc[:,['index']+nname['TAG'].tolist()]
        newnms = [ var_map(j,nname) for j in dat_unt.columns]
        dat_unt.columns = newnms
        datgrp[i]= dat_unt
    return({'datos':odat,'vindices':varindx,'grupos':lgrps,'datgrps':datgrp})
#
#
if __name__ == "__main__":
    main()
