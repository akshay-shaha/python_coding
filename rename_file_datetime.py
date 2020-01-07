### Python Script used to rename datetime value in file names.
#############

### Parameter Names
### Update Parameter Values as per requirement
new_datetime='20191220'
dir_path=r'G:\COBI Delivery\Source Onboarding\RPR'

### Rename Code, Exception Handling, and Return Message
import os
import re
os.chdir(dir_path)
print ("\nOperating inside path -"+dir_path+"...\n")
list_of_files = os.listdir(dir_path)
for file in list_of_files:
    s=file.rfind('_')
    e=file.rfind('.')
    if e==-1:
        print ("File "+file+" skipped as '.' not present")
    elif re.match("[0-9]{4}",file[s+1:e]) or re.match("[0-9]{8}",file[s+1:e]):
        tgt_file=file[0:s+1]+new_datetime+file[e:]
        try:
            os.rename(file,tgt_file)
            print ("File "+file+" renamed to "+tgt_file)
        except:
            print ("File "+file+" could not be renamed to "+tgt_file)
            pass
    else:
        tgt_file=file[0:e]+'_'+new_datetime+file[e:]
        try:
            os.rename(file,tgt_file)
            print ("File "+file+" renamed to "+tgt_file)
        except:
            print ("File "+file+" could not be renamed to "+tgt_file)
            pass
