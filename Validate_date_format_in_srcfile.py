### Library
import pandas as pd
import os
import re
import datetime

### Parameters
dda_full_path=r'G:\COBI Delivery\HBO DDA for D&I KETL v1.1.xlsx'
dda_sheet_nm='Specification data set(s)'
dda_hdr_for_tblnms='Dataset'
dda_hdr_for_clmnnms='Attribute'
dda_hdr_for_datatypes='Data Type Oracle'
src_file_folder=r'G:\COBI Delivery\Source Onboarding\HBO'
src_name='HBO'
sample_row_cnt=10000

### Code
df=pd.read_excel(dda_full_path,dda_sheet_nm)
df.rename(columns={dda_hdr_for_datatypes:'Data_Type_Oracle'}, inplace=True)
filelist=os.listdir(src_file_folder)
os.chdir(src_file_folder)
print "\n Reading sample",sample_row_cnt,"rows to validate date-part[dd-MM-yyyy] only....."
serial_num=0
for tblnm,colnm in zip(df[dda_hdr_for_tblnms][df.Data_Type_Oracle.isin(['DATE','TIMESTAMP(0)','TIMESTAMP(6)'])],df[dda_hdr_for_clmnnms][df.Data_Type_Oracle.isin(['DATE','TIMESTAMP(0)','TIMESTAMP(6)'])]):
        tblnm=src_name+'_'+tblnm
        file_found_flg=0
        serial_num=serial_num+1
        for filenm in filelist:
            if re.match(tblnm,filenm):
                    file_found_flg=1
                    print "\n ",str(serial_num),".",filenm,"-",colnm
                    try:
                        src_df=pd.read_csv(filenm,sep=';',usecols=[colnm],nrows=sample_row_cnt,low_memory=False)
                    except:
                        print "\t\t",colnm," not present in ",filenm
                        break
                    vld_cnt=0
                    invld_cnt=0
                    sample_invld_dt=''
                    invld_msg=''
                    for dt in src_df[colnm][src_df[colnm].notnull()]:
                            try:
                                dt_trim=dt.strip()[:10]
                                datetime.datetime.strptime(dt_trim,'%d-%m-%Y')
                                vld_cnt=vld_cnt+1
                            except:
                                invld_cnt=invld_cnt+1
                                sample_invld_dt=dt
                    if invld_cnt>0:
                        invld_msg="\t*******example---"+sample_invld_dt
                    print "\t\tDD-MM-YYYY format",vld_cnt,"\n\t\tInvalid format   ",invld_cnt,invld_msg
        if file_found_flg==0:
                print "\n ",str(serial_num),". No file found for",tblnm
                    
