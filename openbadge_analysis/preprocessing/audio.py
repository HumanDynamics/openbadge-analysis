import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import pandas as pd
import scipy.stats as stats
import numpy as np
import seaborn as sns
import os,sys
import itertools 
import datetime
import scipy.signal as signal
import matplotlib.dates as mdates
from sklearn.neighbors.kde import KernelDensity
## if you want to use optimization for kde, uncomment the following two lines.
# from sklearn.grid_search import GridSearchCV
# from sklearn.cross_validation import LeaveOneOut

def get_meet_sec(df_meet):
    """
    return a df with a datetime index rounded to second level.
    """
    df_meet_sec = df_meet.copy()
    df_meet_sec.index = df_meet_sec.index.map(lambda x: x.replace(microsecond=0))
    return df_meet_sec

def get_meet_flt(df_meet, window=8):
    """
    data processing with median filter.
    !!!require more consideration
    """
    df_flt = pd.rolling_median(df_meet, window=window)
    return df_flt

def get_df_cor(df_meeting, sel_users):
    """
    return df of correlation of two selected users.
    """
    df_meeting_sel = df_meeting[sel_users]    
    df_sel_sec = get_meet_sec(df_meeting_sel)
    ts = []
    cors = []
    for i in df_sel_sec.index.unique():
        frame = df_sel_sec.loc[i]
        dfa, dfb = frame[sel_users[0]], frame[sel_users[1]]
        p = stats.pearsonr(dfa, dfb)[0]
        if not np.isnan(p):
            ts.append(i)
            cors.append(p)
    df_cor = pd.DataFrame(np.vstack([ts, cors]).T, columns=['time', 'cor'])
    df_cor.set_index('time', inplace=True)
    return df_cor

def get_kde_pdf(X, bandwidth=2, step=.1, num_samples=200, optimize=False):
    """
    return kde and pdf from a data sample
    """
    if len(X) ==0 :
        return [],np.array([]),[]
    if optimize:
        bandwidths = 10 ** np.linspace(-1, 1, 10)
        grid = GridSearchCV(KernelDensity(kernel='gaussian'), {'bandwidth': bandwidths}, 
                            cv=LeaveOneOut(len(X)))
        grid.fit(X[:, None]);
        kde = KernelDensity(kernel='gaussian', bandwidth=grid.best_params_['bandwidth']).fit(X[:,None])    
    else:
        kde = KernelDensity(kernel='gaussian', bandwidth=2).fit(X[:,None]) 
    pdf = np.exp( kde.score_samples(np.arange(0, 100, step)[:,None]) )
    samples = kde.sample(num_samples)            
    return kde, np.array(pdf), samples

def get_seps(dt_nys, prox=0.001, step=0.1, num_samples=200, bandwidth=2):
    """
    return cut-off points for all users
    """
    seps = []
    prox = 0.01
    for idx, user in enumerate(dt_nys):
        ns, ys = dt_nys[user]
        cond_nonezero = len(ns)==0 or len(ys)==0
        kden, pns, nss = get_kde_pdf(ns, bandwidth, step, num_samples)
        kdey, pys, yss = get_kde_pdf(ys, bandwidth, step, num_samples)       
        
        pys[pys<=prox] = 0
        pns[pns<=prox] = 0

        sep = -1        
        if not cond_nonezero:
            for i in np.arange(int(100/step)-1, 0, -1):
                if pys[i-1] < pns[i-1]  and pys[i] >= pns[i]:
                    sep = i * step
                    break        
        seps.append(sep)
    seps = np.array(seps)
    seps[seps == -1] = seps[seps != -1].mean()
    return seps

def get_kldistance(dt_nys, bandwidth=2, prox=0.001, step=0.1, num_samples=200, plot=False, figsize=(12,8)):
    """
    only for 4-user situations
    calculate kl-distance of two distributions (D_t and D_s)
    """
    klds, seps = [], []
    if plot is True:
        fig, axs = plt.subplots(2,2,figsize=figsize,) 
        plt.tight_layout(h_pad=4)
    for idx, user in enumerate(dt_nys):
        ns, ys = dt_nys[user]
        cond_nonezero = len(ns) == 0 or len(ys) ==0
        kden, pns, nss = get_kde_pdf(ns, step=step, num_samples=num_samples, bandwidth=bandwidth)
        kdey, pys, yss = get_kde_pdf(ys, step=step, num_samples=num_samples, bandwidth=bandwidth)
        
        kldistance = stats.entropy(pns, pys) if not cond_nonezero else np.nan
        if not np.isinf(kldistance) and not np.isnan(kldistance):
            klds.append(kldistance)

        pys[pys<=prox] = 0
        pns[pns<=prox] = 0

        sep = -1        
        if not cond_nonezero:
            for i in np.arange(int(100/step)-1, 0, -1):
                if pys[i-1] < pns[i-1]  and pys[i] >= pns[i]:
                    sep = i * step
                    break
        seps.append(sep)
        
        if plot is True:
            ax = axs.flatten()[idx]
            sns.distplot(nss, label='Silent',  kde=False, norm_hist=True, ax=ax)
            sns.distplot(yss, label='Talking', kde=False, norm_hist=True, ax=ax)
            ax.set_title('%s kl-dist:%.2f' % (user, kldistance) )    
            ax.set_xlabel('')
            if not cond_nonezero:
                ax.axvline(x=sep)
                ax.annotate('best sep val: %.1f' % sep, xy=(sep, 0.1), xytext=(sep+5, 0.1), 
                        arrowprops= dict(facecolor='black', shrink=0.0001))
            ax.legend()
            
    seps = np.array(seps)
    seps[seps == -1] = seps[seps != -1].mean()
    return klds, seps

def get_ts_distribution(df_meet, df_spk):
    """
    get distributions for all subjects when they talk or keep silent
    """
    dt_nys = {}
    for user in df_meet.columns:
        ns = df_spk.loc[df_spk.speaker != user][user]
        ys = df_spk.loc[df_spk.speaker == user][user]
        dt_nys[user] = [ns, ys]
    return dt_nys

def get_spk_genuine(df_meet, thre):
    """
    get genuine spk
    """
    df_meet_sec = get_meet_sec(df_meet)
    df_cor = df_meet_sec.groupby(df_meet_sec.index).corr().dropna()
    df_cor = pd.DataFrame((df_cor >= thre).T.all())
    df_cor.reset_index(inplace=True)
    df_cor.columns = ['datetime', 'member', 'val']
    ## Find those people whoes correlation with others are all higher than thre
    df_cor = df_cor.pivot(index='datetime', columns='member', values='val')
    df_mean_ori = df_meet_sec.groupby(df_meet_sec.index).agg(np.mean)
    df_std_ori = df_meet_sec.groupby(df_meet_sec.index).agg(np.std)
    df_mean = pd.DataFrame(df_mean_ori.T.idxmax(), columns=['speaker'])
    ## combine 'correlation' and 'volume' to locate the speaker
    df_comb = df_mean.merge(df_cor, left_index=True, right_index=True)
    ## df_comb_sel contains the speaker information 
    idx = [df_comb.ix[i, u ] for i,u in enumerate(df_comb.speaker)]
    df_comb_sel = df_comb[idx][['speaker']]
    ## get speakers' mean
    df_spk_mean = df_comb_sel.merge(df_mean_ori, left_index=True, right_index=True)
    ## get their std
    df_spk_std = df_comb_sel.merge(df_std_ori, left_index=True, right_index=True)
    return df_spk_mean, df_spk_std

def get_spk_all(df_flt, df_spk_mean, df_spk_std, bandwidth=2):
    """
    get all spk
    """
    df_flt_sec = get_meet_sec(df_flt)
    gps_mean = df_flt_sec.groupby(df_flt_sec.index).mean()
    gps_std = df_flt_sec.groupby(df_flt_sec.index).std()
    speak_all = [] 
    nys_mean = get_ts_distribution(df_flt, df_spk_mean)
    nys_std = get_ts_distribution(df_flt, df_spk_std)
    seps_mean = get_seps(nys_mean, bandwidth=bandwidth)
    seps_std = get_seps(nys_std, bandwidth=bandwidth)
    for i, k in enumerate(df_flt.columns):
        volume_mean = seps_mean[i]
        volume_std = seps_std[i]
        # print '[get_speak_all] user sep_val:', k, volume_mean, volume_std
        df_std = pd.DataFrame(gps_std[gps_std[k] >= volume_std])
        df_mean = pd.DataFrame(gps_mean[gps_mean[k] >= volume_mean])
        df_mean_add = pd.DataFrame(gps_mean.loc[df_std.index])
        df_tmp = pd.concat([df_mean, df_mean_add])
        df_tmp.drop_duplicates(inplace=True)
        df_tmp['speaker'] = k
        speak_all.append(df_tmp)        
    df_speak_all = pd.concat(speak_all)
    ## miss a small fraction from tpspeak()
    # df_tmp = pd.concat([df_speak_all, df_spk_mean])
    # df_speak_all = df_tmp.drop_duplicates()
    return df_speak_all.sort_index(), seps_mean, seps_std

def get_spk_real(df_flt, df_speak_all, thre):
    """
    get real spk
    """
    df_mean_raw = df_speak_all[df_speak_all.index.duplicated(keep = False)].sort_index()
    df_mean_raw.reset_index(inplace=True)
    vals = [df_mean_raw.ix[i, u ] for i, u in enumerate(df_mean_raw.speaker)]
    df_mean_sel = df_mean_raw[['datetime', 'speaker',]]
    df_mean_sel['val'] = vals
    df_mean = df_mean_sel.pivot(index='datetime', columns='speaker', values='val')
    potential_spks = df_mean.columns.tolist()
    volume_spks = [ "%s_vol" % i for i in potential_spks]
    df_mean.columns = volume_spks
    df_meet_sec = get_meet_sec(df_flt)
    ## Notie: dropna() will cause true speakers to be ignored. use fillna instead.
    df_cor = df_meet_sec.groupby(df_meet_sec.index).corr().fillna(2)
    df_comb = df_mean.merge(df_cor, left_index=True, right_index=True)
    ## I know it is hard to read... I just do not want to write for loop
    # the logic is: it returns true only when the correlation with those guys with higher volume are all smaller than the threshold
    idxs_mean = [ False if u not in potential_spks or np.isnan(df_comb.ix[i]['%s_vol' % u]) else (df_comb.ix[i][ df_comb.ix[i][volume_spks][df_comb.ix[i][volume_spks] > df_comb.ix[i]['%s_vol' % u]].index.map(lambda x: x.rstrip('_vol'))] < thre).all() for i, u in enumerate(df_comb.index.get_level_values('member'))]
    df_ms = df_comb[idxs_mean]
    target_cols = ['member']
    target_cols.extend(volume_spks)
    df_ms = df_ms.reset_index(level='member')[target_cols]
    new_cols = ['speaker']
    new_cols.extend(potential_spks)
    df_ms.columns = new_cols
    df_ss = df_speak_all[~df_speak_all.index.duplicated(keep = False)].sort_index()
    df_speak_real = pd.concat([df_ss, df_ms]).reset_index()
    df_speak_real.set_index('datetime', inplace=True)
    return df_speak_real.sort_index()

  

