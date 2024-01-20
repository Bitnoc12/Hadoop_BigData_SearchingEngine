#创建文件并进行编写
def create_and_savefile(text,filename):
  try:
    with open(filename,'w') as file:
      file.write(text)
    print(f"文本已成功保存到文件{filename}")
  except IOError as e:
        print(f"保存文件时发生错误: {e}")

def read_file_and_split():
   #文本内容
   temp = ""
   #文件数，用于命名区分
   filecount = 0
   #行数量，因为文件里的line[0]是从0开始，这里为了实现就从-1开始
   linecount = -1
   #打开文件
   with open('C://0.data//sentences.txt','r') as file:
    #循环文本
    for line in file:
      temp += line
      linecount += 1
      #当已经达到10000行
      if linecount==9999:
         #设置存储地址
         fileroot = "C://0.data//sentences//"
         filename = f"sentences{filecount}.txt"
         fileroot += filename
         #调用函数存储
         create_and_savefile(temp,fileroot)
         #重置
         temp = ""
         filecount += 1 
         linecount = -1
    
    #这里是剩余的达不到9999
    if temp:
       #设置存储地址
       fileroot = "C://0.data//sentences//"
       filename = f"input{filecount}.txt"
       fileroot += filename
       #调用函数存储
       create_and_savefile(temp,fileroot)
      

print("start")       
read_file_and_split()
print("end")

#另一个版本
fileInput = "D:\\bigData\\sentences\\sentences.txt"
root = "D:\\PythonProject\\splitSentencesFile\\sentencesMFile"


SPLIT_NUM = 10000






f = open(fileInput)
lines = f.readlines()


fileId = 1
outputFileName = str(fileId).zfill(3) + ".txt"
outputFilePath = root + "\\" + outputFileName


output = open(outputFilePath, 'a')


sentencesNum = 1
row = 1


for line in lines:


    output.write(line)


    if  row % SPLIT_NUM == 0 and sentencesNum != len(lines):
       
        print(f"file_{fileId} has {row} rows, Done!")
       
        output.close()


        fileId += 1
        outputFileName = str(fileId).zfill(3) + ".txt"
        outputFilePath = root + "\\" + outputFileName


        output = open(outputFilePath, 'a')


        sentencesNum += 1
        row = 1


        continue


    sentencesNum += 1
    row += 1


sentencesNum -= 1
row -= 1


print(f"file_{fileId} has {row} rows, Done!\n")


print(f"sentencesNum = {sentencesNum}, splitNum = {SPLIT_NUM}")
print(f"fileNum = {fileId}\n")


output.close()


f.close()


