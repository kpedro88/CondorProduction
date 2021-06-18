from Condor.Production.jobSubmitter import jobSubmitter

def submitJobs():  
    mySubmitter = jobSubmitter()
    mySubmitter.run()
    
if __name__=="__main__":
    submitJobs()
