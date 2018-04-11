import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns
import sys,os 
src_dir = os.path.join(os.getcwd(), os.pardir, 'openbadge_analysis/preprocessing')
sys.path.append(src_dir)
from audio import *


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

def para_window(df_meet, thre, figsize=(15,8)):
    xs, ys = [], []
    for i in range(1, 30):
        window = i
        df_flt = get_meet_flt(df_meet, window=window)
        df_sec = get_meet_sec(df_flt)
        df_cor = df_sec.groupby(df_sec.index).corr().dropna()
        np_cor = np.triu(df_cor[df_cor >= thre].as_matrix(), 1)
        xs.append(window)
        ys.append(len(np_cor))

    df = pd.DataFrame(np.vstack([xs, ys]).T, columns=['window', '# high correlation'])
    df.set_index('window', inplace=True)
    df.plot(figsize=figsize, marker='s', linestyle='--')
    
def para_thre_cor(df_flt, figsize=(15,8)):
    ks, ns = [], []
    xs = np.arange(0, 1, 0.1)
    for thre in xs:
        # print thre,
        df_spk_mean, df_spk_std = get_spk_genuine(df_flt, thre)
        nys_mean = get_ts_distribution(df_flt, df_spk_mean)
        klds, seps_mean = get_kldistance(nys_mean, plot=False, prox=0.001)
        ks.append(np.mean(klds))
        ns.append(len(df_spk_mean))
    fig, ax = plt.subplots(figsize=figsize)
    l1 = ax.plot(xs, ks, 's-', label='Mean KL distance')
    ax2 = ax.twinx()
    l2 = ax2.plot(xs, ns, '^--', label='# of genuine spk slots', color='#FDAB6D')
    for yt in ax2.get_yticklabels():
        yt.set_color('#FDAB6D')
    lns = l1 + l2
    labels = [l.get_label() for l in lns]
    ax.legend(lns, labels, loc=1)
    ax.set_xlabel('Correlation thresholds')

def plot_raw_cor(df_meeting, sel_users, figsize=(30,15), thre=0.5, plot=True, cor_detail=False):
    """
    plot raw voice signal of selected users and annoted time spans when their correlation is no less than the given threshold.
    """
    df_meeting_sel = df_meeting[sel_users] 
    df_cor = get_df_cor(df_meeting, sel_users)
    df_cor_t = df_cor[df_cor>=thre]
    # calculate a index showing the degree of high correlation
    cor_index = len(df_cor_t.dropna())
    if plot:
        legend = []
        ps = []
        legend.extend(sel_users)
        legend.append('corr >= %.1f' % thre)
        ax = df_meeting_sel.plot(alpha=0.7,  style=['-', '--'], figsize=figsize, marker='o', markersize=1.5)
        dt_cor = df_cor_t.dropna().to_dict()['cor']
        for k in dt_cor:
            k1 = k + datetime.timedelta(seconds=1)
            ps.append([k, k1])
            y = dt_cor[k] * 100 if cor_detail else -1
            ps.append([y,y])
            ps.append('s-')
        plt.plot(*ps, markersize=0, linewidth=5 , color='g')    
        plt.legend(legend, title='')
        plt.title('# high correlation frames: %d' % cor_index)
    return cor_index

def plot_sec(df_meet, time, figsize=(30,15), raw=False, sec=False, cor=False, stat=False):
    """
    plot the raw/filtered signal of a given second.
    @modified 2018-02-23 04:48:13
    """
    df_meet.loc[time].plot(figsize=figsize)
    df_meetsec = get_meet_sec(df_meet)
    if raw:
        print df_meet.loc[time]
    if sec:
        print df_meetsec.loc[time]
    if cor:
        print df_meetsec.loc[time].corr()
    if stat:
        print df_meetsec.loc[time].agg([np.mean, np.std])

    return df_meet.loc[time]

def plot_spk(df_flt, df_speak, seps=None, show_trace=False, show_std=False, beg=0, dur=50, rotation=0, interval=5, markersize=1.5, figsize=(30,15), title='', alpha=1, spkplot_gap=1):
    """
    plot the spk informaiotn of either genuine spk, all spk, or real spk.
    """
    idxes = df_speak.index.unique()
    idxes = get_meet_sec(df_flt).index
    sbeg = str(idxes[beg*20])    
    send = str(idxes[beg*20+dur*20])   
    df_speak = df_speak.loc[sbeg: send]
    n_sub = len(df_flt.columns)
    n_row = n_sub + 1
    
    ## Cannot set [sbeg: send] due to pandas
    df_flt_part = df_flt.loc[:send]
    fig, axes = plt.subplots(n_row, 1, figsize=figsize, sharex=True)
    axs = df_flt_part.plot(figsize=figsize, subplots=True, linewidth=1, marker='o', 
                     markersize=markersize, alpha=alpha, title=title, ax=axes[:n_sub])

    ### add std
    if show_std:
        df_sec = get_meet_sec(df_flt_part)
        df_std = df_sec.groupby(df_sec.index).std()
        dt_std = {}
    
    colors = []
    dt_uc = {}
    dt_ps = {}
    for comb in zip(axs, df_flt.columns):
        ax, u = comb
        l = ax.lines[0]
        dt_uc[u] = l.get_color()
        dt_ps[u] = []
        if show_std:
            dt_std[u] = []
    subjects = sorted(dt_uc.keys(), reverse=True)
    
    if show_std:
        for k in df_sec.index.unique():
            k1 = k + datetime.timedelta(seconds=1)
            for u in df_sec.columns:
                # add std
                stdu = df_std.ix[k, u]
                dt_std[u].append([k, k1])
                dt_std[u].append([stdu, stdu])
    
    
    for k in df_speak.index:
        k1 = k + datetime.timedelta(seconds=1)
        us = df_speak.loc[df_speak.index == k].speaker.tolist()
        for u in us:
            y = -1 * spkplot_gap * ( 1 + subjects.index(u) )
            dt_ps[u].append([k, k1])
            dt_ps[u].append([y, y])
            
    nax = axes[n_sub]

    for i,u in enumerate(df_flt.columns):
        c = dt_uc[u]
        params = dt_ps[u]
        axs[i].plot(*params, linewidth=5, color=c)
        if seps is not None:
            axs[i].axhline(seps[i], linestyle= '--', color='black', alpha=0.8)
        axs[i].set_ylim([-10,60])
        axs[i].set_ylabel('Volume')
        axs[i].grid(axis='x', which='major', alpha=0.5, linestyle=':') 
        
        # add std
        if show_std:
            params_std = dt_std[u]
            axs[i].plot(*params_std, linewidth=3, color='black', linestyle='--')
        
        if show_trace and len(params) != 0:
            nax.axhline(params[1][0], linestyle=':' , color=c )
        nax.plot(*params, linewidth=spkplot_gap*20, color=c);
        nax.set_ylim([0, -1*spkplot_gap*(n_sub+1) ])
        nax.set_yticklabels('')        
        nax.xaxis.set_major_locator(mdates.SecondLocator(interval=interval))
        dateformatter = ':%S' if dur <= 60 else '%M:%S'
        nax.xaxis.set_major_formatter(mdates.DateFormatter(dateformatter))

    nax.grid(axis='x', which='major', alpha=0.5, linestyle='--')
    nax.set_xlabel('Time')
    nax.set_ylabel('Speaker')
    ## This is just a work-around. Something should be wrong with df.plot (pd version 0.22.)
    nax.set_xlim([sbeg, send])
    plt.xticks(rotation=rotation)
    plt.tight_layout()
    return sbeg, send    

