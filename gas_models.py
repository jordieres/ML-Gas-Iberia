#
# Starting from the pickle file elaborated by gas_preprocess.py
# and the file with the description of variables we will train 
# ML models to predict those variables.
#
# The trained models will be stored in a folder with a short version in a 
# short subdirectory and a summary in pickle format with the index of models.
#
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
        help="Filename for the pickle container. Row1: dat; Row2: Vars; Row3: Groups; Row4: Dat per group")
    ap.add_argument("-g", "--group", type=int, required=True,
        help="Interesting group to be developed further")
    ap.add_argument("-o", "--output", type=str, required=True,
        help="Directory to place the created models")
    ap.add_argument("-t", "--target", type=str, required=True,
        help="Comma separated list of varables to be modelled")
    ap.add_argument("-n", "--nump", type=int, required=True,
        help="Number of parameters for reduced models")
    ap.add_argument("-d", "--date", type=str, required=False,
        help="Date for training validation split: YYYY-MM-DD", 
        default="2023-03-27")
    ap.add_argument("-l", "--list", action='store_true', required=False,
        help="List the gropus exisitng in pickle container")
    ap.add_argument("-w", "--vars", action='store_true', required=False,
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
    nump    = args["nump"]
    verbose = args["verbose"]
    dates   = datetime.datetime.strptime(args["date"],"%Y-%m-%d")
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
                ",".join([str(j) for j in lgrps]) + \
                ". We can't proceed further")
        sys.exit(2)
    else:
        dat = datg[grp]
        tvrs= dat.columns.tolist()
    listv = []
    if len(svmdl) > 0:
        tmplistv = [i for i in svmdl.split(',')]
        for j in tmplistv:
            r = re.compile(j)
            nlist= list(filter(r.match, tvrs))
            listv = listv +nlist
    if verbose > 0:
        print("- Interesting list of variables to be modelled:")
        print("     "+",\n     ".join(listv))
    #
    if listg:
        print("- List of Groups with recorded data:")
        print("  " + ",".join(lgrps))
    #
    if slistv:
        print("- List of Variables in group "+str(grp)+" with recorded data:")
        print("    " + ",\n    ".join(dat.columns.tolist()))
    #
    mdlT    = {}
    for il in listv:
        mdl = train_model(dat,vidx,dmdl,il,nump,dates,grp,verbose)
        if verbose > 1:
            print(mdl)
        mdlT[il]=mdl
    if verbose > 0:
        print(mdlT)
    ecopkl  = dmdl+"/summary_mdls_"+ datetime.datetime.strftime(
            datetime.datetime.now(),"%Y-%m-%dT%H:%M:%S") + ".pkl"
    #
    with open(ecopkl, "wb") as output_file:
        pickle.dump(mdlT,output_file)
        pickle.dump(vidx,output_file)
        pickle.dump(datg,output_file)
    return(None)
#
def train_model(dat,vars,dout,vmodel,nump,lngcut,grp,vrb):
    # Se toma la serie desde el inicio hasta el 27/3/2023 como criterio para entrenamiento
    # Después test.
    # lngcut = datetime.datetime.strptime('2023-03-27 00:00:00',"%Y-%m-%d %H:%M:%S")
    DFtrain= dat.loc[dat['Time:ISO'] < lngcut,list(set(dat.columns) - 
                                                    set(['Time:ISO']))]
    DFtest = dat.loc[dat['Time:ISO'] >= lngcut,list(set(dat.columns) - 
                                                    set(['Time:ISO']))]
    h2o.init(verbose=False)
    h2o_frame = h2o.H2OFrame(DFtrain)
    x = h2o_frame.columns
    y = vmodel
    x.remove(y)
    #
    h2o_automl = H2OAutoML(sort_metric='mse',max_runtime_secs=30*60,
                           seed=666,verbosity=None)
    h20_err    = h2o_automl.train(x=x, y=y, training_frame=h2o_frame)
    ltmdls     = h2o.automl.get_leaderboard(h2o_automl,extra_columns ="ALL")
    #
    h2o_frame_test = h2o.H2OFrame(DFtest)
    y_pred   = h2o_automl.predict(h2o_frame_test)
    y_actual = h2o.H2OFrame(DFtest[[y]])
    yerr     = h2o_mean_squared_error(y_actual, y_pred)
    varimp   = h2o_automl.varimp(use_pandas=True)
    if vrb > 0:
        print("   - Full Ensemble Model MSE:"+str(yerr))
        print(varimp)
    bm       = h2o_automl.leader
    # metalearner = h2o.get_model(h2o_automl.leader.metalearner().model_id)
    perf     = bm.model_performance(h2o_frame_test)
    if vrb > 1:
        print("   - Full Single Model MSE:"+str(perf))
    #
    nam_mdl  = h2o.save_model(model=bm, path=dout, force=True)
    if vrb > 0:
        print("   - Name of Full Model:"+ nam_mdl)
    mdlT     = {'y':y,'x':x,'yorg':y_actual,'ypred':y_pred,'mse':yerr,
                'mdl_imp':varimp,'grp':grp,'perf':extract_perf(perf),
                'mdl_nam':nam_mdl}
    #
    # Restricted model ...
    sweepModel= H2OModelSelectionEstimator(mode="backward", # backward, maxr, maxrsweep, allsubsets
                                        max_predictor_number=nump, seed=666)
    gmdls = sweepModel.train(x=x, y=y, training_frame=h2o_frame)
    bcoef = sweepModel.coef()[nump]
    lclsR = list(bcoef.keys())[1:]
    datR  = dat[['Time:ISO',vmodel]+lclsR]
    #
    DFtrainR= datR.loc[datR['Time:ISO'] < lngcut,list(set(datR.columns) - 
                                                        set(['Time:ISO']))]
    DFtestR = datR.loc[datR['Time:ISO'] >= lngcut,list(set(datR.columns) - 
                                                        set(['Time:ISO']))]    
    h2o_frameR = h2o.H2OFrame(DFtrainR)
    x = h2o_frameR.columns
    x.remove(y)
    #
    h2o_automlR = H2OAutoML(sort_metric='mse',max_runtime_secs=15*60,
                            seed=666,verbosity=None)
    h20_errR = h2o_automlR.train(x=x, y=y, training_frame=h2o_frameR)
    ltmdlsR  = h2o.automl.get_leaderboard(h2o_automlR,extra_columns ="ALL")
    h2o_frame_testR = h2o.H2OFrame(DFtestR)
    y_predR  = h2o_automlR.predict(h2o_frame_testR)
    y_actualR= h2o.H2OFrame(DFtestR[[y]])
    yerrR    = h2o_mean_squared_error(y_actualR, y_predR)
    varimpR  = h2o_automlR.varimp(use_pandas=True)
    #
    bmR      = h2o_automlR.leader
    # metalearnerR= h2o.get_model(h2o_automlR.leader.metalearner().model_id)
    perfR    = bmR.model_performance(h2o_frame_testR)
    if vrb > 1:
        print("   - Restricted Single Model MSE:"+str(perfR))
    #
    nam_mdlR = h2o.save_model(model=bmR, path=dout+'/short/', force=True)
    if vrb > 0:
        print("   - Name of Restricted Model:"+ nam_mdlR)
    mdlR     = {'y':y,'x':x,'yorg':y_actualR,'ypred':y_predR,'mse':yerrR,
                'mdl_imp':varimpR,'grp':grp,'perf':extract_perf(perfR),
                'mdl_nam':nam_mdlR}          
    res = {'Full':mdlT,'Rest':mdlR}
    return(res)
#
def extract_perf(obj):
    res = {}
    if 'model' in obj._metric_json.keys():
        res['model'] = obj._metric_json['model']
    if 'model_category' in obj._metric_json.keys():
        res['model_category']   = obj._metric_json['model_category']
    if 'nobs' in obj._metric_json.keys():
        res['nobs']  = obj._metric_json['nobs']
    if 'algo' in obj._metric_json.keys():
        res['algo']  = obj._algo
    if '_on' in obj._metric_json.keys():
        res['on']    = obj._on
    if 'residual_degrees_of_freedom' in obj._metric_json.keys():
        res['residual_degrees_of_freedom'] = obj._metric_json[
            'residual_degrees_of_freedom']
    if 'MSE' in obj._metric_json.keys():
        res['MSE']   = obj._metric_json['MSE']
    if 'RMSE' in obj._metric_json.keys():
        res['RMSE']  = obj._metric_json['RMSE']
    if 'r2' in obj._metric_json.keys():
        res['r2']    = obj._metric_json['r2']
    if 'rmsle' in obj._metric_json.keys():
        res['rmsle'] = obj._metric_json['rmsle']
    if 'mae' in obj._metric_json.keys():
        res['mae']   = obj._metric_json['mae']
    if 'mean_residual_deviance' in obj._metric_json.keys():
        res['mean_residual_deviance']= obj._metric_json[
            'mean_residual_deviance']
    if 'residual_deviance' in obj._metric_json.keys():
        res['residual_deviance']= obj._metric_json['residual_deviance']
    if 'AIC' in obj._metric_json.keys():
        res['AIC']   = obj._metric_json['AIC']
    if 'null_deviance' in obj._metric_json.keys():
        res['null_deviance']    = obj._metric_json['null_deviance']
    return(res)
#
#
#
if __name__ == "__main__":
    main()
