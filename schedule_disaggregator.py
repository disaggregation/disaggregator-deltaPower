import time, os

# Chang to current work directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(os.path.dirname(os.path.abspath(__file__)))

def job(logfolder="../data/"):
    print ("DISAGGREGATOR started at " + time.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        try:
            execfile("./deltaP.py")
        except:
            exec(open("./deltaP.py").read())
    except:
        print("DISAGGREGATOR failed")
    return
    
while True:
  job()
  time.sleep(60)

