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
from os.path import exists
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
    ap.add_argument("-p", "--pickle", type=str, required=True,
        help="Filename for the pickle container. Row1: dat; Row2: Vars; Row3: Groups; Row4: Dat per group")
    ap.add_argument("-g", "--group", type=str, required=True,
        help="Interesting group to be developed further")
    ap.add_argument("-o", "--output", type=str, required=True,
        help="Directory to place the created models")
    ap.add_argument("-t", "--target", type=str, required=True,
        help="Comma separated list of varables to be modelled")
    ap.add_argument("-l", "--list", type=str, required=False,
        help="List the gropus exisitng in pickle container")
    ap.add_argument("-w", "--vars", type=str, required=False,
        help="List the var names in the interesting group")
    ap.add_argument('-v', nargs='?', action=VAction, dest='verbose')
    #
    args    = vars(ap.parse_args())
    # print('{} --> {}'.format(sys.argv[1:], args))
    pfich1  = args["pickle"]
    grp     = args["group"]
    dmdl    = args["output"]
    svmdl   = args["target"]
    listg   = args["list"]
    slistv  = args["vars"]
    verbose = args["verbose"]
    if verbose is None:
        verbose = 0
    #
    pkl_is  = exists(pfich1)
    if not pkl_is:
        print(" *** Error: File " + pfich1 + "does not exist. We can't follow up")
        sys.exit(2)
    if not os.path.isdir(dmdl):
        print(" *** Error: Directory " + dmdl + "does not exist. We can't follow up")
        sys.exit(2)
    if verbose > 1:
        print("- Loading datasets ...")
    with open(pfich1, "rb") as input_file:
        rg01 = pickle.load(input_file)
        del rg01
        vidx = pickle.load(input_file)
        lgrps= pickle.load(input_file)
        datg = pickle.load(input_file)
    if verbose > 1:
        print("- Datasets loaded ...")
    if grp not in lgrps:
        print(" *** Error: Group "+ str(grp)+ " not included in " + \
                ",".join(lgrps) + ". We can't proceed further")
        sys.exit(2)
    else:
        dat = datg[grp]
        del datg
        tvrs= dat.columns.tolist()
    listv = []
    if len(slistv) > 0:
        tmplistv = [i for i in slistv.split(',')]
        for j in tmplistv:
            r = re.compile(j)
            nlist= list(filter(r.match, tvrs))
            listv.append(nlist)
    if verbose > 0:
        print("- Interesting list of variables to be modelled:")
        print("  "+",".join(listv))
    #
    
    return(None)
#
#
#
if __name__ == "__main__":
    main()
