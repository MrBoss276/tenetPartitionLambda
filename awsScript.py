import os
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

with ZipFile('partition_lambda.zip', 'w', compression=ZIP_DEFLATED) as zipObj:
    zipObj.write("lambda_function.py")
    os.chdir("venv/lib/python3.8/site-packages")
    for folderName, subfolders, filenames in os.walk("./"):
        for filename in filenames:
            filePath = os.path.join(folderName, filename)
            zipObj.write(filePath[2:])
    zipObj.close()
