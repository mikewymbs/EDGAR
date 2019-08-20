import pandas as pd
import os
dt = int(os.environ['SLURM_ARRAY_TASK_ID'])
logdate = 'log2014120'+str(dt)
mapcik = pd.read_csv('/home/mwymbs/lf/map_cik.csv')
mf_ciks = mapcik.CIK.drop_duplicates().reset_index(drop=True)
df = pd.concat([mf_ciks], axis=1)
string = '/home/mwymbs/lf/Raw/ip_' + logdate + '.csv'
lf = pd.read_csv(string)
lf=lf.merge(df,how='inner',left_on='cik',right_on='CIK')
lf.to_csv('/home/mwymbs/lf/small/mf'+logdate+'.csv',index=False) 
lf = lf[lf.botcd==0]
lf = lf[['ip','date','size','cik','accession','extention']].drop_duplicates().reset_index(drop=True)
soph_ip = pd.read_csv('/home/mwymbs/mflog/sophist_IP.csv')
mif = pd.read_csv('/home/mwymbs/mflog/master_index_file.csv')
locs = pd.read_csv('/home/mwymbs/mflog/all_loc_info.csv')

if lf.shape[0]>0:
	bigDF = pd.DataFrame(columns =['cik','date','size','IP','accession','extention','country_gbcountry','prob_country','country_gbstate','state_gbstate','prob_state','country_gbcity','state_gbcity','city_gbcity','prob_city','Form Type','Date Filed'])
	for q in range(0,lf.shape[0]):
		newDF = pd.DataFrame(columns =['cik','date','size','IP','accession','extention','country_gbcountry','prob_country','country_gbstate','state_gbstate','prob_state','country_gbcity','state_gbcity','city_gbcity','prob_city','Form Type','Date Filed'])
		line = lf.loc[lf.index[q],:]
		line_data = line[['cik','date','extention','botcd','accession','size']]
		line_ip = locs[locs.IP==line.ip]
		loc_data = line_ip[['IP','country_gbcountry','prob_country','country_gbstate','state_gbstate','prob_state','country_gbcity','state_gbcity','city_gbcity','prob_city']]
		mif_line = mif[mif.Filename==line.accession]
		mif_data = mif_line[['Form Type','Date Filed']]
		newDF.loc[0,'cik'] = line_data.cik
		newDF.loc[0,'date'] = line_data.date
		newDF.loc[0,'size'] = line_data['size']
		newDF.loc[0,'accession'] = line_data['accession']
		newDF.loc[0,'extention'] = line_data['extention']
		if loc_data.shape[0] >0:
			newDF.loc[0,'IP'] = loc_data.IP[loc_data.index[0]]
			newDF.loc[0,'country_gbcountry'] = loc_data.country_gbcountry[loc_data.index[0]]
			newDF.loc[0,'prob_country'] = loc_data.prob_country[loc_data.index[0]]
			newDF.loc[0,'country_gbstate'] = loc_data.country_gbstate[loc_data.index[0]]
			newDF.loc[0,'state_gbstate'] = loc_data.state_gbstate[loc_data.index[0]]
			newDF.loc[0,'prob_state'] = loc_data.prob_state[loc_data.index[0]]
			newDF.loc[0,'country_gbcity'] = loc_data.country_gbcity[loc_data.index[0]]
			newDF.loc[0,'state_gbcity'] = loc_data.state_gbcity[loc_data.index[0]]
			newDF.loc[0,'city_gbcity'] = loc_data.city_gbcity[loc_data.index[0]]
			newDF.loc[0,'prob_city'] = loc_data.prob_city[loc_data.index[0]]
		if mif_data.shape[0]>0:
			newDF.loc[0,'Form Type'] = mif_data['Form Type'][mif_data.index[0]]
			newDF.loc[0,'Date Filed'] = mif_data['Date Filed'][mif_data.index[0]]
			td = pd.to_datetime(line_data.date) - pd.to_datetime(mif_data['Date Filed'][mif_data.index[0]])
			newDF.loc[0,'Filing Age'] = int(td.days)
		if line.ip in set(soph_ip.IP):
			newDF.loc[0,'sophist']=1
		else:
			newDF.loc[0,'sophist']=0
		bigDF=bigDF.append(newDF)
bigDF.to_csv('/home/mwymbs/lf/nobotmf/'+logdate+'.csv',index=False) 