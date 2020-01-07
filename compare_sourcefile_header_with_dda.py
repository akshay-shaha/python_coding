### Python Script used to compare sourcefile header with dda fields
#############

### Parameter Names
### Update Parameter Values as per requirement

dda_full_path=r'G:\COBI Delivery\Source Onboarding\RPR\RPR DDA for D&I KETL v0.2.xlsx'
sheet_name='Specification data set(s)'
srcfile_path=r'G:\COBI Delivery\Source Onboarding\RPR\Source files'

### Import library
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import os

### Generate array with source file names and header

print "\nReading Source file headers ... "
srcfile_hdr=[]
list_of_files = os.listdir(srcfile_path)
for file in list_of_files:
    f=open(srcfile_path+'\\'+file,'r')
    source_header=f.readline().replace('"',r'\"').rstrip()
    lst=[file,source_header]
    srcfile_hdr.append(lst)

### Generate array with DDA tables and field names as header
print "\nReading DDA sheet fields ... "
df=pd.read_excel(dda_full_path,sheet_name)
dda_hdr=[]
st=r'\"'
prev_tblnm=''
for i in df.index:
    tblnm=str(df['Dataset'][i])
    colnm=str(df['Attribute'][i])
    if tblnm<>prev_tblnm and i>0:
        lst=[prev_tblnm,st[:-3]]
        dda_hdr.append(lst)
        st=r'\"'
    st=st+colnm+r'\";\"'
    prev_tblnm=tblnm
lst=[prev_tblnm,st[:-3]]
dda_hdr.append(lst)    

### Compare DDA field with source file header
print "\nComparing DDA sheet fields with source file headers ... \n"
cntr=0
for d in range(len(dda_hdr)):
    cntr=cntr+1
    str_cntr=str(cntr)
    flg=0
    for s in range(len(srcfile_hdr)):
        filenm=srcfile_hdr[s][0]
        filepart=filenm[filenm.find('_')+1:filenm.rfind('_')]
        if filepart==dda_hdr[d][0]:
            flg=1
            if srcfile_hdr[s][1]==dda_hdr[d][1]:
                print "\n "+str_cntr+". DDA fields matched for "+dda_hdr[d][0]+" with header of "+filenm
            else:
                print "\n"+str_cntr+". *****DDA fields MIS-MATCH for "+dda_hdr[d][0]+" with header of "+filenm
                print "\t  File header "+srcfile_hdr[s][1]
                print "\t  DDA fields  "+dda_hdr[d][1]
    if flg==0:
        print "\n"+str_cntr+". *****NO SOURCE FILE FOUND for "+dda_hdr[d][0]

