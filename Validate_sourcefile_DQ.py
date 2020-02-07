######################################################################################
## This Python script can validate source files using metadata present in DDA document. 
##
## Script sections :
## 1. Importing Library   2. Variables declaration and Parameters assignment
## 3. Function defination 4. Function Execution
##
## v0.1 - Draft     - Number,Date validation - 20200127 - Akshay Shaha 
## v0.2 - OOP       - OOP, all sources - 20200204 - Akshay Shaha 
## v0.3 - Notnull   - Not null,SOH char - 20200205 - Akshay Shaha 
######################################################################################

### 1. Library
import pandas as pd
import os
import re
import datetime
import time

### 2. Global Variable Declaration
global decimal_seperator
global datepart_format

### Parameter Assignment
src_file_folder=r'G:\COBI Delivery\Source Onboarding\all\csv'
dda_file_folder=r'G:\COBI Delivery\Source Onboarding\all\dda'
dda_sheet_nm='Specification data set(s)'
dda_hdr_for_tblnms='Dataset'
dda_hdr_for_clmnnms='Attribute'
dda_hdr_for_datatypes='Data Type Oracle'
renamed_hdr_for_datatypes='Data_Type_Oracle'
dda_hdr_for_scale='Scale'
dda_hdr_for_precision='Precision'
dda_hdr_for_nullable='Indicator Nullable'
renamed_hdr_for_nullable='Indicator_Nullable'
csv_field_seperator=';'
i_decimal_seperator=','
i_datepart_format='%d-%m-%Y'
special_char_soh='\x01'

### 3. Function Defination ###
## Function to load dda excel sheet into DataFrame
def f_load_dda(dda_file_nm,dda_sheet_nm):
    dda_df=pd.read_excel(dda_file_nm,dda_sheet_nm)
    return dda_df

## Function to rename header in DataFrame
def f_rename_df_header(df,src_colnm,tgt_colnm):
    df.rename(columns={src_colnm:tgt_colnm}, inplace=True)
    return df

## Function to generate file list 
def f_generate_filelist(src_file_folder):
    os.chdir(src_file_folder)
    filelist=os.listdir(src_file_folder)
    return filelist

## Function to load and return DataFrame using source file, for specified column-list.
def f_load_source_file(fname,cname_list,csv_field_seperator):
    err_flg=0
    src_df=[]
    try:
        src_df_chunk=pd.read_csv(fname,sep=csv_field_seperator,usecols=cname_list,low_memory=False,dtype='str',chunksize=100000,na_values='',keep_default_na=False)
    except:
        err_flg=1
    else:
        src_df=pd.concat(chunk for chunk in src_df_chunk)
    return src_df,err_flg

## Function to validate datepart 
def f_chk_datepart_format(dt):
    dt_trim=str(dt).strip()[:10]
    try:
            datetime.datetime.strptime(dt_trim,datepart_format)
            vld_flg=1
    except:
            vld_flg=0
    return vld_flg

## Function to return length of integer part of number
def f_calc_intgr_len(nmbr):
    nmbr=str(nmbr).lstrip('0')
    s=nmbr.find(decimal_seperator)
    if s==-1:
        actual_intgr_length=len(nmbr)
    else:
        actual_intgr_length=len(nmbr[:s])
    return actual_intgr_length

## Function to return length of decimal part of number
def f_calc_decml_len(nmbr):
    nmbr=str(nmbr).rstrip('0')
    s=nmbr.find(decimal_seperator)
    if s==-1:
        actual_decml_length=0
    else:
        actual_decml_length=len(nmbr[s+1:])
    return actual_decml_length

## Function to validate datepart and print valid count, invalid count, and sample
def f_validate_datepart_format(src_df, cname, i_datepart_format):
    global datepart_format
    datepart_format=i_datepart_format
    invld_dt_sample=''
    tgt_df=src_df[[cname]].copy()
    tgt_df['datepart_valid_flg']=tgt_df[cname][tgt_df[cname].notnull()].apply(f_chk_datepart_format)
    invld_dt_cntr=len(tgt_df.query('datepart_valid_flg==0'))
    vld_cntr=len(tgt_df.query('datepart_valid_flg==1'))
    print "\t\tDate-part Format valid records",vld_cntr
    if invld_dt_cntr>0:
        invld_dt_sample=tgt_df.query('datepart_valid_flg==0')[cname][-1:].to_string(index=False).encode('ascii')
        print "\t\tDate-part Format invalid for",invld_dt_cntr,"records.\t\t\t*******example---",invld_dt_sample

## Function to calculate and print valid count, invalid count, and sample
def f_validate_precision_scale(src_df, cname, precision, scale, i_decimal_seperator):
    global decimal_seperator
    decimal_seperator=i_decimal_seperator
    if scale!=scale:                   ### Null handling in scale
            scale=0
    else:
            scale=int(scale)           ### Float to Int coversion
    if precision!=precision:           ### Null handling in precision, setting default length 38 if not specified
            precision=38
    else:
            precision=int(precision)   ### Float to Int coversion    
    invld_precision_sample=''
    invld_scale_sample=''
    invld_precision_cntr=0
    invld_scale_cntr=0
    vld_cntr=0
    allwd_intgr_length=precision-scale
    allwd_decml_length=scale
    tgt_df=src_df[[cname]].copy()
    tgt_df['actual_intgr_length']=tgt_df[cname][tgt_df[cname].notnull()].apply(f_calc_intgr_len)
    tgt_df['actual_decml_length']=tgt_df[cname][tgt_df[cname].notnull()].apply(f_calc_decml_len)
    invld_precision_cntr=len(tgt_df.query('actual_intgr_length>@allwd_intgr_length'))
    invld_scale_cntr=len(tgt_df.query('actual_decml_length>@allwd_decml_length'))
    vld_cntr=len(tgt_df.query('actual_intgr_length<=@allwd_intgr_length and actual_decml_length<=@allwd_decml_length'))
    print "\t\tPrecision and Scale valid records",vld_cntr
    if invld_precision_cntr>0:
        invld_precision_sample=tgt_df.query('actual_intgr_length>@allwd_intgr_length')[cname][-1:].to_string(index=False).encode('ascii')
        print "\t\tValue larger than allowed precision for",invld_precision_cntr,"records.\t\t*******example---",invld_precision_sample
    if invld_scale_cntr>0:
        invld_scale_sample=tgt_df.query('actual_decml_length>@allwd_decml_length')[cname][-1:].to_string(index=False).encode('ascii')
        print "\t\tValue larger than allowed scale for",invld_scale_cntr,"records\t\t\t*******example---",invld_scale_sample

## Function to validate not null constraint and print valid count, invalid count, and sample record number
def f_validate_notnull_constraint(src_df, cname):
    null_cntr=len(src_df[cname][src_df[cname].isna()])
    vld_cntr=len(src_df[cname][src_df[cname].notna()])
    print "\t\tNot Null constraint valid records",vld_cntr
    if null_cntr>0:
        invld_rownum_sample=list(src_df[cname][src_df[cname].isna()][-5:].index.values)
        invld_rownum_sample=[ index+2 for index in invld_rownum_sample ] ## adding 2 to sync with file row number
        print "\t\tNot Null constraint invalid for",null_cntr,"records.\t\t\t*******example---Row Numbers",invld_rownum_sample

## Function to validate whether special characters exist in source file
def f_file_search_string(filenm, special_char_soh):
    soh_rownum_sample=[]
    soh_line_cntr=0
    rownum=0
    src_file_object=open(filenm)
    for line in src_file_object:
        rownum=rownum+1
        if special_char_soh in line:
            soh_rownum_sample.append(rownum)
            soh_line_cntr=soh_line_cntr+1
    if soh_line_cntr==0:
        print "\t\tFile is free of SOH character"
    else:
        print "\t\tFile contains SOH character in",soh_line_cntr,"records.\t\t\t*******example---Row Numbers",soh_rownum_sample[0:5]

### 4. Function execution ###
dda_list=os.listdir(dda_file_folder)
for file in dda_list:
    if 'dda' in file.lower() and file.lower().endswith('xlsx') and '$' not in file:      ## Selecting only dda files and ignoring temporary files.
        dda_file_nm=file
        end_positn=dda_file_nm.find('_')                ## dda name should start with "<SOURCE NAME>_" or "<SOURCE NAME> "
        if end_positn==-1:
            end_positn=dda_file_nm.find(' ') 
        if end_positn==-1:
            print "\nSource Name not identified in DDA -",dda_file_nm,"\n--------------\n--------------"
            continue
        src_name=dda_file_nm[:end_positn].upper()
        os.chdir(dda_file_folder)
        print "\nDDA excel found -",dda_file_nm
        print "Source Name derived -",src_name
        #Load DDA
        dda_df=f_load_dda(dda_file_nm,dda_sheet_nm)
        dda_df=dda_df[dda_df[dda_hdr_for_tblnms].notna()]
        #Rename DDA column - remove spaces
        dda_df=f_rename_df_header(dda_df,dda_hdr_for_datatypes,renamed_hdr_for_datatypes)
        dda_df=f_rename_df_header(dda_df,dda_hdr_for_nullable,renamed_hdr_for_nullable)
        #Generate source file list
        filelist=f_generate_filelist(src_file_folder)
        # Data Validation - PRECISION/SCALE, DATEPART FORMAT
        src_start_time=time.time()
        print "\nValidating all rows...\n"
        serial_num=0
        filter_datatypes=dda_df[renamed_hdr_for_datatypes].str.contains('NUMBER|FLOAT|DECIMAL|DOUBLE|DATE|TIMESTAMP',case=False)
        filter_notnull=dda_df[renamed_hdr_for_nullable].str.contains('NO',case=False)
        dda_df=dda_df[filter_datatypes | filter_notnull]
        for t in dda_df[dda_df[dda_hdr_for_tblnms].notnull()][dda_hdr_for_tblnms].str.encode('ascii').unique():
            tblnm_fltr=dda_df[dda_hdr_for_tblnms]==t
            colnm_list=dda_df[dda_hdr_for_clmnnms][tblnm_fltr].str.encode('ascii').tolist()
            tblnm=src_name+'_'+t
            file_found_flg=0
            for filenm in filelist:
                if re.match(tblnm+'_'+"[0-9]{8}",filenm):
                    file_found_flg=1
                    serial_num=serial_num+1
                    print str(serial_num),".",filenm
                    f_file_search_string(filenm,special_char_soh)
                    src_df, err_flg=f_load_source_file(filenm,colnm_list,csv_field_seperator)
                    for tblnm,colnm,datatype,scale,precision,nullable in zip(dda_df[dda_hdr_for_tblnms][tblnm_fltr],dda_df[dda_hdr_for_clmnnms][tblnm_fltr],dda_df[renamed_hdr_for_datatypes][tblnm_fltr],dda_df[dda_hdr_for_scale][tblnm_fltr],dda_df[dda_hdr_for_precision][tblnm_fltr],dda_df[renamed_hdr_for_nullable][tblnm_fltr]):
                        serial_num=serial_num+1
                        print str(serial_num),".",filenm,"-",colnm
                        if err_flg==1:
                            print "\t\t",colnm,"not present in",filenm
                            continue
                        if 'NUMBER' in datatype.upper() or 'FLOAT' in datatype.upper() or 'DECIMAL' in datatype.upper() or 'DOUBLE' in datatype.upper(): 
                            f_validate_precision_scale(src_df, colnm, precision, scale, i_decimal_seperator)
                        if 'DATE' in datatype.upper() or 'TIMESTAMP' in datatype.upper():
                            f_validate_datepart_format(src_df, colnm, i_datepart_format)
                        if 'NO' in nullable.upper():
                            f_validate_notnull_constraint(src_df, colnm)
            if file_found_flg==0:
                    serial_num=serial_num+1
                    print str(serial_num),". No file found for",tblnm
        src_end_time=time.time()
        delta=round(src_end_time-src_start_time,1)
        print "\n",src_name,": Executed in",delta,"seconds\n\n--------------\n--------------"
