import ast, json, random, argparse
import os, sys, datetime, shutil
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
ap = argparse.ArgumentParser()
ap.add_argument("-f", "--file", type=str, required=True,
	help="Filename with excel structure to be processed")
args = vars(ap.parse_args())
pfich = args["file"]
effts = pd.read_excel(pfich)
#
yrs = [''] + [str(j) for j in sorted(effts['Año'].unique().tolist())]
cntr= [''] + sorted(effts['CodCentral'].unique().tolist())
try:
    sns.set_style('darkgrid')
    row0_spacer1, row0_1, row0_spacer2 = st.columns((.1, 3, .1))
    row0_1.title('Parámetros  Gas Iberia')
    row0_1.write('')
    
    row1_spacer1, row1_1, row1_spacer2, row1_2, row1_spacer3,row1_3,row1_spacer4 = st.columns(
    (.1,1.,.1,2.,0.1,1.,.1))
    error = 0
    with row1_1:
        yfrm = st.selectbox("Seleccione Año de Interés",yrs)
        if len(yfrm) == 0:
            st.error("Por favor, seleccione un elemento de la lista")
            error= 1
    with row1_3:
        codc   = st.selectbox("Seleccione Central", cntr)
        if len(codc) > 0:
            ddt = effts.loc[(effts['Año']==int(yfrm))&(effts['CodCentral']==codc),\
                              [ 'Año','Q','CodCentral','CodGrupo','dur','npar',\
                                'QPrd','NHrs','MTBF','MTTR','%DSP']]
            grps = sorted(ddt['CodGrupo'].unique().tolist())
            for igr in grps:
                for iq in range(1,5):
                    idx = (ddt['Año']==int(yfrm)) & (ddt['Q']== iq) & \
                          (ddt['CodGrupo']==igr) & (ddt['CodCentral']==codc)
                    if len(ddt.loc[idx,'QPrd']) == 0:
                        qprd= pd.Period(str(yfrm)+'Q'+str(iq),freq='Q')
                        nhrs= round((qprd.end_time-qprd.start_time).total_seconds()/3600.)
                        nrw = { 'Año':[yfrm],'Q':[iq],'CodCentral':[codc], \
                                'CodGrupo':[igr],'dur':[0.],'npar':[1],'QPrd':[qprd],\
                                'NHrs':[nhrs],'MTBF':[nhrs],'MTTR':[0.],'%DSP':[100.]}
                        ddt = pd.concat([ddt,pd.DataFrame(nrw)],axis=0)
    if 'ddt' in locals():
        ddt.reset_index(drop=True, inplace=True)
    # pdb.set_trace()
    if len(yfrm) > 0 and len(codc) > 0 :
        ifrm = int(yfrm)
        row2_spacer1, row2_2, row2_spacer3 = st.columns((.1,3,.1)) 
        # print('Año:',yfrm,' ','Central:',codc)
        with row2_2:
            # print('Y:'+yfrm+' Central:'+codc)
            # print(ddt)
            if ddt.shape[0] > 0:
                    fig1= px.bar(x=ddt['CodGrupo'],y=ddt['MTBF'],color=ddt['QPrd'],\
                                barmode = 'group',\
                                labels={"x": "Código del Grupo","y":"MTBF","color":codc})    
                    st.plotly_chart(fig1)
                    fig2= px.bar(x=ddt['CodGrupo'],y=ddt['MTTR'],color=ddt['QPrd'],\
                                barmode = 'group',\
                                labels={"x": "Código del Grupo","y":"MTTR","color":codc})    
                    st.plotly_chart(fig2)
                    fig3= px.bar(x=ddt['CodGrupo'],y=ddt['%DSP'],color=ddt['QPrd'],\
                                barmode = 'group',\
                                labels={"x": "Código del Grupo","y":"% Disp.","color":codc})    
                    st.plotly_chart(fig3)
                    
except URLError as e:
    st.error(
        """
        **This tool requires internet access.**

        Connection error: %s
        """
        % e.reason
    )
