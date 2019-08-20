import pandas as pd
import os
y = int(os.environ['SLURM_ARRAY_TASK_ID'])
ips =  pd.read_csv('/home/mwymbs/Downloads/IPs/Locations/IP_loc_inputs/all_ip_'+str(y)+'.csv')
integer_isp=pd.read_csv('/home/mwymbs/Downloads/IPs/Locations/isp_ip_integer.csv')
def write_value_all(ind):
    ips.loc[ind,'isp'] = above_cutoff.isp[above_cutoff.index[0]]
    ips.loc[ind,'organization'] = above_cutoff.organization[above_cutoff.index[0]]
    ips.loc[ind,'autonomous_system_number'] = above_cutoff.autonomous_system_number[above_cutoff.index[0]]
    ips.loc[ind,'autonomous_system_organization'] = above_cutoff.autonomous_system_organization[above_cutoff.index[0]]
    ips.loc[ind,'prob_isp'] = 1
    ips.loc[ind,'prob_org'] = 1
    ips.loc[ind,'prob_asn'] = 1
    ips.loc[ind,'prob_aso'] = 1
def write_value_gb2(ind):
    ips.loc[ind,'isp'] = gbisp.isp[gbcity.index[0]]
    ips.loc[ind,'organization'] = gborg.organization[gbcity.index[0]]
    ips.loc[ind,'autonomous_system_number'] = gbasn.autonomous_system_number[gbcity.index[0]]
    ips.loc[ind,'autonomous_system_organization'] = gbaso.autonomous_system_organization[gbstate.index[0]]
    ips.loc[ind,'prob_isp'] = gbisp.loc[gbisp.index[0],'chances']/int(above_cutoff.chances.sum())
    ips.loc[ind,'prob_org'] = gborg.loc[gborg.index[0],'chances']/int(above_cutoff.chances.sum())
    ips.loc[ind,'prob_asn'] =  gbasn.loc[gbasn.index[0],'chances']/int(above_cutoff.chances.sum())
    ips.loc[ind,'prob_aso'] =  gbaso.loc[gbaso.index[0],'chances']/int(above_cutoff.chances.sum())
def write_value_gb(ind):
    ips.loc[ind,'isp'] = gbisp.isp[gbisp.index[0]]
    ips.loc[ind,'organization'] = gborg.organization[gborg.index[0]]
    ips.loc[ind,'autonomous_system_number'] = gbasn.autonomous_system_number[gbasn.index[0]]
    ips.loc[ind,'autonomous_system_organization'] = gbaso.autonomous_system_organization[gbaso.index[0]]
    ips.loc[ind,'prob_isp'] = gbisp.loc[gbisp.index[0],'chances']/256
    ips.loc[ind,'prob_org'] = gborg.loc[gborg.index[0],'chances']/256
    ips.loc[ind,'prob_asn'] =  gbasn.loc[gbasn.index[0],'chances']/256
    ips.loc[ind,'prob_aso'] =  gbaso.loc[gbaso.index[0],'chances']/256
def write_value_all2(ind):
    ips.loc[ind,'isp'] = above_cutoff.isp
    ips.loc[ind,'organization'] = above_cutoff.organization
    ips.loc[ind,'autonomous_system_number'] = above_cutoff.autonomous_system_number
    ips.loc[ind,'autonomous_system_organization'] =above_cutoff.autonomous_system_organization
    ips.loc[ind,'prob_isp'] = 1
    ips.loc[ind,'prob_org'] = 1
    ips.loc[ind,'prob_asn'] = 1
    ips.loc[ind,'prob_aso'] = 1

for ind in range(0,ips.shape[0]):
    if ind %1000 ==0:
        print ind
    ip = ips.IP[ind].split('.')
    if int(ip[0]) in set(integer_isp.start1):
        tempcity = integer_isp[integer_isp.start1==int(ip[0])]
        above_cutoff2 = tempcity[tempcity.end2 >= int(ip[1])]
        above_cutoff2 = above_cutoff2[above_cutoff2.start2 <= int(ip[1])]
        tempcity2 = tempcity[tempcity.start2==int(ip[1])]
        if len(ip) > 2: #short IP problem
            above_cutoff = tempcity2[tempcity2.end3 >= int(ip[2])]
            above_cutoff = above_cutoff[above_cutoff.start3 <= int(ip[2])]
            above_cutoff= above_cutoff.fillna('')
            if tempcity2.shape[0] > 0:
                tempcity0 = tempcity2.loc[max(tempcity2.index),:] #last line problem
                if tempcity0.start3 <= int(ip[2]):
                    above_cutoff = tempcity0
                    write_value_all2(ind)
                    continue
                    print "shouldn't see this text"
            if above_cutoff.shape[0]>1:
                above_cutoff.loc[above_cutoff.index[0],'chances'] = above_cutoff.loc[above_cutoff.index[0],'end4']+1
                above_cutoff.loc[above_cutoff.index[len(above_cutoff.index)-1],'chances'] = 256 - above_cutoff.loc[above_cutoff.index[len(above_cutoff.index)-1],'start4']
                mid_ip = above_cutoff[above_cutoff.start3 == above_cutoff.end3]
                for i in mid_ip.index:
                    above_cutoff.loc[i,'chances'] = 1+above_cutoff.loc[i,'end4'] - above_cutoff.loc[i,'start4']
                gbisp = above_cutoff.groupby(['isp'])['chances'].sum().reset_index()
                gbisp = gbisp[gbisp.chances ==gbisp.chances.max()]
                gborg = above_cutoff.groupby(['organization'])['chances'].sum().reset_index()
                gborg = gborg[gborg.chances ==gborg.chances.max()]
                gbasn = above_cutoff.groupby(['autonomous_system_number'])['chances'].sum().reset_index()
                gbasn = gbasn[gbasn.chances ==gbasn.chances.max()]
                gbaso = above_cutoff.groupby(['autonomous_system_organization'])['chances'].sum().reset_index()
                gbaso= gbaso[gbaso.chances ==gbaso.chances.max()]
                write_value_gb(ind)
                continue
            if above_cutoff.shape[0]==1:
                write_value_all(ind)
                continue
            if above_cutoff.shape[0]==0:
                if above_cutoff2.shape[0]==1:
                    above_cutoff = above_cutoff2
                    write_value_all(ind)
                    continue
                if above_cutoff2.shape[0]>1:
                    for i in above_cutoff2.index:
                        if above_cutoff2.loc[i,'end2'] == int(ip[1]):
                            if above_cutoff2.loc[i,'end3'] > int(ip[2]): #should be able to exactly identify in this scenario
                                above_cutoff = above_cutoff2.loc[i,:]
                                break
                        else:
                            above_cutoff = above_cutoff2.loc[i,:]
                    write_value_all2(ind)
                    continue
                if above_cutoff2.shape==0:
                    print ind, "whats going on here"
            else: 
                if above_cutoff.shape[0]==0:
                    if above_cutoff2.shape[0]==1:
                        above_cutoff = above_cutoff2
                        write_value_all(ind)
                        continue
                    else:
                        print "stop this here", str(ind)
                        break
        if len(ip) == 2:
            above_cutoff = above_cutoff2
            if above_cutoff.shape[0]==1:
                write_value_all(ind)
                continue
            if above_cutoff.shape[0]==0:
                print ind, "whats going on here, 2 digit IP"
            else:
                above_cutoff['chances'] = 256*(above_cutoff['end3']-above_cutoff['start3']) + above_cutoff['end4']-above_cutoff['start4']+1
                above_cutoff.loc[above_cutoff.index[0],'chances'] = above_cutoff.loc[above_cutoff.index[0],'end4']+1
                above_cutoff.loc[above_cutoff.index[len(above_cutoff.index)-1],'chances'] = 256 - above_cutoff.loc[above_cutoff.index[len(above_cutoff.index)-1],'start4']
                gbisp = above_cutoff.groupby(['isp'])['chances'].sum().reset_index()
                gbisp = gbisp[gbisp.chances ==gbisp.chances.max()]
                gborg = above_cutoff.groupby(['organization'])['chances'].sum().reset_index()
                gborg = gborg[gborg.chances ==gborg.chances.max()]
                gbasn = above_cutoff.groupby(['autonomous_system_number'])['chances'].sum().reset_index()
                gbasn = gbasn[gbasn.chances ==gbasn.chances.max()]
                gbaso = above_cutoff.groupby(['autonomous_system_organization'])['chances'].sum().reset_index()
                gbaso= gbaso[gbaso.chances ==gbaso.chances.max()]
                print "got here nice!" , ind 
                write_value_gb2(ind)
                continue
ips.to_csv('/home/mwymbs/Downloads/IPs/Locations/IP_loc_outputs/ip_org_'+str(y)+'.csv',index=False)
