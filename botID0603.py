import pandas as pd
import os
dt = int(os.environ['SLURM_ARRAY_TASK_ID'])
logdate = 'log200603'+str(dt)
string = '/home/mwymbs/lf/Raw/' + logdate + '.csv'
lf = pd.read_csv(string)
lf = lf[lf['code'] < 300]
lf = lf[lf['code']>199]
lf = lf[lf['idx']==0]
lf=lf.reset_index(drop=True)
freq = lf.groupby(['ip'])['time'].count().reset_index()
hifreq = freq[freq.time>499]
botips= set(hifreq.ip)
lf.time = pd.to_datetime(lf.time)
lf['minute'] = 0
for k in range(0,lf.shape[0]):
    lf.loc[k,'minute'] = lf.time[k].minute + lf.time[k].hour*60
permin = lf.groupby(['ip','minute','cik'])['accession'].count().reset_index()
permin1 = permin.groupby(['ip','minute'])['cik'].count().reset_index()
bot = list(set(permin1[permin1.cik>2].ip))
min25 = lf.groupby(['ip','minute'])['time'].count().reset_index()
bot25 = list(set(min25[min25.time>24].ip))
crawl = lf[lf['crawler']==1].ip
bot_codes = pd.DataFrame(columns=['ip','botcd'])
for k in botips:
	bot_codes= bot_codes.append({'ip':k,'botcd':1},ignore_index=True)
for m in crawl:
	if m not in set(bot_codes.ip):
		bot_codes= bot_codes.append({'ip':m,'botcd':2},ignore_index=True)
for j in bot25:
	if j not in set(bot_codes.ip):
		bot_codes= bot_codes.append({'ip':j,'botcd':3},ignore_index=True)
for c in bot:
	if c not in set(bot_codes.ip):
		bot_codes= bot_codes.append({'ip':c,'botcd':4},ignore_index=True)
lf = lf.merge(bot_codes,how='left',on='ip')
lf.botcd = lf.botcd.fillna(0)
lf.to_csv('/home/mwymbs/lf/Output/ip_'+logdate+'.csv',index=False)