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
ap.add_argument("-w", "--vars", type=str, required=True,
	help="Filename with excel variable structure to be processed")
ap.add_argument("-d", "--data", type=str, required=True,
	help="Filename with excel dataset to be processed")
#
args    = vars(ap.parse_args())
pfich   = args["data"]
sheet_to_df_map = pd.read_excel(pfich, sheet_name=None)
sheet_to_df_map.keys()
odat    = sheet_to_df_map['Sheet1']
#
pfich   = args["vars"]
sheet_to_df_map = pd.read_excel(pfich, sheet_name=None)
sheet_to_df_map.keys()
varindx = sheet_to_df_map['server_taglist_cpi']
#
lgrps   = varindx['GLOBAL_CODE_ID'].unique()
