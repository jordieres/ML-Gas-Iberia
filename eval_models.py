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
import ast, json, random, argparse, re
import os, sys, datetime, shutil
import datetime, math, pickle, pdb
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
        help="Filename for the pickle container of models. \
            Row1: Models, Row2: Var dictionary, Row3: data per group")
    ap.add_argument("-f", "--file", type=str, required=True,
        help="Filename pickle for records to be assesed. \
              One row for gropus and other for data to be evaluated. \
              Row1: groups; Row2: Dat per group")
    ap.add_argument("-g", "--group", type=int, required=True,
        help="Interesting group for the prediction")
    ap.add_argument("-o", "--output", type=str, required=True,
        help="File to place the predictions")
    ap.add_argument("-t", "--target", type=str, required=True,
        help="Variable under interest for prediction")
    ap.add_argument("-m", "--model", type=str, required=False,
        help="Pattern of models to be used for prediction")
    ap.add_argument("-l", "--list", nargs='?', action=VAction, 
        help="List of models with performance for information",
        required=False, dest='list')
    ap.add_argument('-v', nargs='?', action=VAction, dest='verbose')
    #
    args    = vars(ap.parse_args())
    pfich1  = args["pickle"]
    f_eval  = args["file"]
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
    feval_is= exists(f_eval)
    if not feval_is:
        print(" *** Error: File " + f_eval + " does not exist. Nothing to assess")
        sys.exit(2)
    if verbose > 1:
        print("- Loading datasets ...")
    with open(pfich1, "rb") as input_file:
        txtmd= pickle.load(input_file)
    mdlT = json.loads(txtmd)
    with open(f_eval, "rb") as input_file:
        datg = pickle.load(input_file)
    lgrps= list(datg.keys())
    #
    if lperf > 0:
        for idx in mdlT.keys():
            relev = re.findall(ovar, idx)
            if len(relev) > 0 and mdlT[idx]['Full']['grp'] == grp:
                prfT = mdlT[idx]['Full']['perf']
                prfR = mdlT[idx]['Rest']['perf']
                xT   = mdlT[idx]['Full']['x']
                namT = mdlT[idx]['Full']['mdl_nam']
                namR = mdlT[idx]['Rest']['mdl_nam']
                xR   = mdlT[idx]['Rest']['x']
                tmdlT= json.loads(mdlT[idx]['Full']['lst_mdls'])['algo']['0']
                tmdlR= json.loads(mdlT[idx]['Rest']['lst_mdls'])['algo']['0']
                print(' Var: {0:>25s}. Type: {1:>25s}.  Num_vars: {2:>3d}  R2: {3:>6.4f} Model: {4:>40s}'.format(
                        idx,tmdlT,len(xT),prfT['r2'],namT))
                print('                                 ' + 
                    'Type: {0:>25s}.  Num_vars: {1:>3d}  R2: {2:>6.4f} Model: {3:>40s}'.format(
                        tmdlR, len(xR),prfR['r2'],namR))
    #
    if len(t_mdl) > 0:
        cltr  = h2o.init()
        idl   = 0
        cmmdl = pd.DataFrame([{'key':'','type':'', 'path':''}])
        res   = {}
        for idx in mdlT.keys():
            relev = re.findall(ovar, idx)
            if len(relev) > 0 and mdlT[idx]['Full']['grp'] == grp:
                tmdlT= json.loads(mdlT[idx]['Full']['lst_mdls'])['algo']['0']
                tmdlR= json.loads(mdlT[idx]['Rest']['lst_mdls'])['algo']['0']
                typ  = 'Full'
                relm = re.findall(t_mdl,tmdlT)
                if len(relm) > 0:
                    idl += 1
                    res, cmmdl = mdl_predict(mdlT,typ,tmdlT,datg[grp],cmmdl,res,idx,idl)
                typ  = 'Rest'
                relm = re.findall(t_mdl,tmdlR)
                if len(relm) > 0:
                    idl += 1
                    res, cmmdl = mdl_predict(mdlT,typ,tmdlR,datg[grp],cmmdl,res,idx,idl)
        h2o.cluster().shutdown()
        cmmdl.reset_index(drop=True,inplace=True)
        # 
        # Write excel sheets inside the book
        with pd.ExcelWriter(preds) as fex:
            for idx in res.keys():
                res[idx].to_excel(fex,sheet_name=idx.split(':')[0], 
                                  index=False)
            cmmdl.to_excel(fex, sheet_name='Models', index=False)
    return(None)
#
def mdl_predict(mdlT,typ,tmdl,datg,cmmdl,res,idx,idl):
    xT   = mdlT[idx][typ]['x']
    namT = mdlT[idx][typ]['mdl_nam']
    model= h2o.load_model(namT)
    DFtst= datg.loc[:,xT]
    h2oft= h2o.H2OFrame(DFtst)
    y_prd= h2o.as_list(model.predict(h2oft))
    y_act= datg.loc[:,[idx]]
    vmdl = 'MDL_{0:>03d}'.format(idl)
    if cmmdl.shape[0] == 1: # Build the res DF
        res[idx] = pd.DataFrame({'Time:ISO':datg.loc[:,'Time:ISO'],
                    'Real_Values':y_act.iloc[:,0].tolist(),
                    vmdl:y_prd.iloc[:,0].tolist()})
        cmmdl    = pd.concat([cmmdl, pd.DataFrame([{'key': vmdl,
                    'type':tmdl,'path': namT}])],axis=0,ignore_index=True)
    else:
        res[idx][vmdl] = y_prd.iloc[:,0].tolist()
        cmmdl    = pd.concat([cmmdl, pd.DataFrame([{'key': vmdl,
                    'type':tmdl,'path': namT}])],axis=0,ignore_index=True)
    return(res,cmmdl)
#
#
#
if __name__ == "__main__":
    main()
