import win32com.client
import pandas as pd
import os
import io
import datetime as dt
from zipfile import ZipFile
from xlwt import Workbook
import re
import extract_msg

#specify emailid from which we will fetch input files. Make sure you write in smaller case.
accepted_emails = ['vbhogle@its.jnj.com','ra-jnjau-commanalyt@its.jnj.com','akoshion@its.jnj.com','ra-conaubw-aupharmcl@its.jnj.com','ra-conaubw-aupharmcl@jnj.com']

accepted_file_formats = ['API','SIGMA','Symbion','SYM','SIG']

def check_format(filename):
    status = False
    index = 0
    for i in range(len(accepted_file_formats)):
        if re.search(accepted_file_formats[i],filename) is None:
            continue
        else:
            status = True
            index = i
            break
    return (status,index)

def extract_zip(file_dir, output_dir, Output_name ,file_month, idx,name='Alteryx Inputs'):
    # Extract_files
    list_files = os.listdir(file_dir)
    os.chdir(file_dir)
    i = 1
    print('Extracting.....')
    for File in list_files:
        if File.endswith('.ZIP') or File.endswith('.zip'):
            with ZipFile(file_dir + '\\' + File, 'r') as z:
                z.extractall(output_dir + '/' + Output_name + ' ' + file_month + '/' + name+'/' + File.rstrip('.zip'))
            print('Saved File : ' + str(File))
            z.close()
    # Saving all excels in one file
    single_excel(file_dir, output_dir,Output_name,file_month,idx)
    return ()

def single_excel(file_dir, output_dir, Output,file_month,idx,name='Alteryx Inputs'):
    print('Creating a single Excel File')
   # Creating temporary Files
    path = output_dir + '/' + name + '/'
    temp_folder = output_dir + '/' +'temp/'
    if os.path.exists(temp_folder) == False:
        os.makedirs(temp_folder)

    # Defining writer for excel file
    os.chdir(output_dir)
    writer = pd.ExcelWriter(Output+'.xlsx')
    list_files = os.listdir(path)

    # Using temporary Folder
    os.chdir(temp_folder)

    for File in list_files:
        try:
            List_files = os.listdir(os.path.join(path,File))
        except:
            List_files = []
        path_1 = os.path.join(path,File)
        for file in List_files:
            if file.lower().endswith('.csv'):
				if i in (2,3):
					print('Opening file {0}'.format(file.lower()))
					data = pd.read_csv(os.path.join(path_1,file),dtype='str')
					data.to_excel(writer, sheet_name=File.rstrip('.ZIP'))
					print('Saved to the excel')
					del(data)
				else:
					print('Opening file {0}'.format(file.lower()))
					data = pd.read_csv(os.path.join(path_1,file))
					data.to_excel(writer, sheet_name=File.rstrip('.ZIP'))
					print('Saved to the excel')
					del(data)

            elif file.lower().endswith('.xls') or file.lower().endswith('.xlsx'):
                print('Opening file {0}'.format(file.lower()))
                file1 = io.open(os.path.join(path_1,file), "r", encoding="utf-8")
                data = file1.readlines()
                # Creating a workbook object
                xldoc = Workbook()
                # Adding a sheet to the workbook object
                sheet = xldoc.add_sheet("Sheet1", cell_overwrite_ok=True)
                # Iterating and saving the data to sheet
                if idx == 0:
                    for i, row in enumerate(data):
                        if i != 0:
                        # Removing the '\n' which comes while reading the file using io.open
                        # Getting the values after splitting using '\t'
                            for j, val in enumerate(row.replace('\n', '').split('\t')):
                                sheet.write(i-1, j, val)
                    xldoc.save('temp.xls')
                    data = pd.read_excel('temp.xls')
                    data.to_excel(writer, sheet_name=File)
                    print('Saved to the excel')
                    del(data)
                else:
                    for i, row in enumerate(data):
                        # Removing the '\n' which comes while reading the file using io.open
                        # Getting the values after splitting using '\t'
                        for j, val in enumerate(row.replace('\n', '').split('\t')):
                            sheet.write(i, j, val)
                    xldoc.save('temp.xls')
                    data = pd.read_excel('temp.xls')
                    data.to_excel(writer, sheet_name=File)
                    print('Saved to the excel')
                    del(data)
    writer.save()
    return()

# Accepted_names = ['']

mydesktop = os.path.expanduser('~') + '\\JNJ\\'
outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

print(dt.datetime.now())
now_time = str(dt.datetime.now().strftime('%B %Y'))
# setup range for outlook to search emails (so we don't go through the entire inbox)
lastWeekDateTime = dt.datetime.now() - dt.timedelta(days=10)
lastWeekDateTime = lastWeekDateTime.strftime('%m/%d/%Y %H:%M %p')

# setting up month for the attachment files
lastmonthDateTime = dt.datetime.now() - dt.timedelta(days=30)
file_month = lastmonthDateTime.strftime('%b %Y')

# Select main Inbox
inbox = outlook.Folders('Commercial Analytics Team')
inbox = inbox.Folders('Inbox')

# Optional:  Select main Inbox, look in subfolder "Test"
# inbox = outlook.GetDefaultFolder(6).Folders["Test"]

messages = inbox.Items

# Only search emails in the time range above:
messages = messages.Restrict("[ReceivedTime] >= '" + lastWeekDateTime + "'")

print('Reading Inbox, including Inbox Subfolders...')

# Download a select attachment ---------------------------------------
# Create a folder to capture attachments.
Myfolder = mydesktop + 'E2E Data Excellence - Extracted_WRS_files\\' + now_time +'\\'
if not os.path.exists(Myfolder): os.makedirs(Myfolder)

try:
    for message in list(messages):
        try:
            # Getting subject of the mail to find the type of invoice
            s = message.subject
            s = str(s)
            print('Sender:', message.sender)
            # Collecting the Email Id of the sender to check in our accepted email id's
            try:
                email_id = message.Sender.GetExchangeUser().PrimarySmtpAddress
            except:
                email_id = 'None'
            print('Total Attachments {0}'.format(len(message.attachments)))
            # Checking the Email id in our directory of accepted emails
            if email_id.lower() in accepted_emails:
                # Checking whether the email subject is relevant to any of the type invoice  and returning the type of invoice
                stat,idx = check_format((s))
                if stat == True:
                    # Give each attachment a path and filename
                    Myfolder_1 = Myfolder + accepted_file_formats[idx] + ' ' + file_month + '\\'
                    if os.path.exists(Myfolder_1) == False:
                        os.makedirs(Myfolder_1)
                    for att in message.Attachments:
                        if att.FileName.endswith('.msg'):
                            outfile_name1 = Myfolder + att.FileName
                            att.SaveASFile(outfile_name1)
                            print('Saved file:', outfile_name1)
                            msg_file = extract_msg.openMsg(outfile_name1)
                            msg_attachments = msg_file.attachments
                            i = 1
                            for atta in msg_attachments:
                                if atta.type == 'msg':
                                    outfile_name2 = Myfolder_1 + "MSG_File " + str(i)
                                    atta.save(customPath=Myfolder_1, customFilename="MSG_File " + str(i))
                                    print('Saved file:', outfile_name2)
                                    i += 1
                                else:
                                    atta.save(customPath=Myfolder_1)

                        else:
                            outfile_name = Myfolder_1 + att.FileName
                            att.SaveASFile(outfile_name)
                            print('Saved file:', outfile_name)
                            extract_zip(Myfolder_1, Myfolder,accepted_file_formats[idx],file_month,idx)
                    # Extracting all the zip files in the directory
                    extract_zip(Myfolder_1, Myfolder,accepted_file_formats[idx],file_month,idx)

                test = os.listdir(Myfolder)


        except Exception as e:
            print("type error: " + str(e))
            x = 1

except Exception as e:
    print("type error: " + str(e))
    x = 1

# Delete unused file types (like .png)-----------------------------------------

test = os.listdir(Myfolder)

for item in test:
    if item.endswith(".png"):
        os.remove(os.path.join(Myfolder, item))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/


