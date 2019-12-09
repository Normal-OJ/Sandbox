# -*- coding: utf-8 -*-
from flask import Flask, request
import os
from os import walk
import json
from flask import jsonify
import demjson
import zipfile
import glob
app = Flask(__name__)
@app.route('/submit',methods=['GET', 'POST'])
def submit():
    languages = ['c', 'cpp', 'py']
    if request.method == 'POST':
        file = request.files['upload_file']#get file
        file.save("C:/test/"+file.filename.split(".")[0]+'.zip')#save file
        
        checker=request.values['checker'] 
        language_id=request.values['languageId'] #0:C,1:C++,2:python3
        language_type=''
        if type(language_id)==int and 0<=language_id<=2:
            language_type=languages[language_id]
        else:
            return "language id wrong-400",400
           
        token=request.values['token']
        submission_id=request.values['submissionId']
        while type(submission_id)!=str:
            return "submission id wrong-400",400
        
        archive = zipfile.ZipFile("C:/test/Submissions.zip", 'r')
        #file_name=archive.filename.split('.')[0]#filename
        archive.extractall('C:/test/')
        archive.close()
        
        archize_src=zipfile.ZipFile('C:/test/'+submission_id+'/src.zip', 'r')
        archize_src.extractall('C:/test/'+submission_id)
        archize_src.close()
        
        archize_testcase=zipfile.ZipFile('C:/test/'+submission_id+'/testcase.zip', 'r')
        archize_testcase.extractall('C:/test/'+submission_id)
        archize_testcase.close()
           
        if len(os.listdir('C:/test/'+submission_id+'/src/'))==0:#len(['1','2','3'])
            return "under src does not have any file-400",400
        else:
            for files in walk('C:/test/'+submission_id+'/src/'):
                print (files)
                file_name=files[2][0].split(".")[0]#get file name
                if file_name=="main":
                    which_file=files[2][0].split(".")[1]#'py'
                    if which_file==language_type:
                        target_path_in_0 = r"C:\test\12345678\0\*.in"
                        target_path_out_0 = r"C:\test\12345678\0\*.out"
                        target_path_in_1 = r"C:\test\12345678\1\*.in"
                        target_path_out_1 = r"C:\test\12345678\1\*.out"
                        folder_in_0=glob.glob(target_path_in_0)
                        folder_out_0=glob.glob(target_path_out_0)
                        folder_in_1=glob.glob(target_path_in_1)
                        folder_out_1=glob.glob(target_path_out_1)
                        
                        if len(folder_in_0)==len(folder_out_0) and len(folder_in_1)==len(folder_out_1):
                            if (os.path.isfile('C:/test/'+submission_id+'/meta.json')):
                                # read file
                                with open('C:/test/'+submission_id+'/meta.json', 'r') as myfile:
                                    data=myfile.read()
                                # parse file
                                obj = json.loads(data)
                                value = json.loads(demjson.encode(obj['cases'][0]))
                                if type(value['caseScore'])==int and type(value['memoryLimit'])==int and type(value['timeLimit'])==int:
                                    return jsonify({'status':'ok','msg':'ok','data':'ok'})
                                else:
                                    return "none int-400",400
                            else:
                                return "no meta data-400",400
                        else:
                            return "0 diff 1-400",400
                    else:
                        return "data type is not match-400",400 
                else:
                    return "none main-400",400
                    
    return "<form method=post enctype=multipart/form-data>" \
            "</br>" \
           "<input type='hidden' name='checker' value='test_checker' />"\
           "<input type='hidden' name='languageId' value=2 />"\
           "<input type='hidden' name='token' value='test_checker' />"\
           "<input type='hidden' name='submissionId' value='12345678' />"\
           "<input type=file name=upload_file>"\
           "<input type=submit value=Upload_zip>"\
           "</form>"
    
if __name__ == '__main__':
    app.run(host='127.0.0.1')