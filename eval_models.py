#
# Starting from the pickle file provided by gas_prepeval.py,
# the pickle file with the summary of the trained models 
# and the file with the description of variables we will 
# assess the provided records from the selected group against 
# the matching models.
#
# The output file will be an excel file 

# (C) UPM.  JOM 2023-11-30
# 
import ast, json, random, argparse
import os, sys, datetime, shutil, pdb
import statistics, smtplib, ssl, re, string
import datetime, math, pickle, dill, pdb
import h2o
#
import numpy as np
import pandas as pd
#
from os.path import exists
from pathlib import Path
from datetime import date, timedelta
from tempfile import mkstemp
from argparse import ArgumentParser
from urllib.error import URLError
from h2o.automl import H2OAutoML
from h2o.model.models.regression import h2o_mean_squared_error
from h2o.estimators import H2OModelSelectionEstimator
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
        help="Filename for the dill container of models. Row1: models; Row3: Dat per group")
    ap.add_argument("-f", "--file", type=str, required=True,
        help="Filename pickle for records to be assesed. One row per group")
    ap.add_argument("-g", "--group", type=int, required=True,
        help="Interesting group for the prediction")
    ap.add_argument("-o", "--output", type=str, required=True,
        help="File to place the predictions")
    ap.add_argument("-t", "--target", type=str, required=True,
        help="Variable under interest for prediction")
    ap.add_argument("-m", "--model", type=str, required=False,
        help="Pattern of models to be used for prediction")
    ap.add_argument("-l", "--list", nargs='?', dest='list',
        help="List of models with performance for information")
    ap.add_argument('-v', nargs='?', action=VAction, dest='verbose')
    #
    args    = vars(ap.parse_args())
    pfich1  = args["pickle"]
    f_xlsx  = args["file"]
    grp     = args["group"]
    preds   = args["output"]
    ovar    = args["target"]
    lperf   = args["list"]
    t_mdl   = args["model"]
    verbose = args["verbose"]
    #
    #
    if verbose is None:
        verbose = 0
    pkl_is  = exists(pfich1)
    if not pkl_is:
        print(" *** Error: File " + pfich1 + " does not exist. We can't follow up")
        sys.exit(2)
    fxlsx_is= exists(f_xlsx)
    if not fxlsx_is:
        print(" *** Error: File " + f_xlsx + " does not exist. Nothing to assess")
        sys.exit(2)
    if verbose > 1:
        print("- Loading datasets ...")
    regs    = pd.read_excel(f_xlsx)
    with open(args["pickle"], "rb") as input_file:
        mdlT = dill.load(input_file)
        vidx = dill.load(input_file)
        datg = dill.load(input_file)
    #
    if lperf > 0:
        r = re.compile(ovar)
        for idx in mdlT.keys():
            relev = list(filter(r.match, idx))
            if len(relev) > 0 and mdlT[idx]['Full']['grp'] == grp:
                prfT = mdlT[idx]['Full']['perf']
                prfR = mdlT[idx]['Rest']['perf']
                xT   = mdlT[idx]['Full']['x']
                namT = mdlT[idx]['Full']['nam_mdlT']
                namR = mdlT[idx]['Rest']['nam_mdlR']
                xR   = mdlT[idx]['Rest']['x']
                print(" Var: {idx:15s}. Num_vars: {len(xT):%3d} R2: {prfT['R2']:%6.4f} Model: {namT:%40s}")
                print(" Var:                . Num_vars: {len(xR):%3d} R2: {prfR['R2']:%6.4f Model: {namR:%40s}")
    #
    if len(t_mdl) > 0:
        rp = re.compile(t_mdl)
        for idx in mdlT.keys():
            relev = list(filter(r.match, idx))
            if len(relev) > 0 and mdlT[idx]['Full']['grp'] == grp:
                relm = list(filter(rp.match,mdlT[idx]['Full']['nam_mdlT']))
                
#
#
#
if __name__ == "__main__":
    main()
