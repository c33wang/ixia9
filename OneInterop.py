import os, warnings
os.chdir('/Users/ubnt/githubrepo/ixia9/')

from ixia.webapi import *

###                            suppress warnings                                 ###
warnings.filterwarnings("ignore")

###                                 login credintials                            ###
webServerAddress = "https://10.1.0.199"
version = "v1"
user = "chun"
password = "ubnt"

###connects to server
api = webApi.connect(webServerAddress, version, None, user, password)
###joins session #26
session = api.joinSession(26)
###run sessions

print "Test is running ......"
result = session.runTest()

save_path = "/Users/ubnt/Documents/testResultszzz.zip"
with open(save_path, "a+") as statsFile:
	api.getStatsCsvZipToFile(result.testId, statsFile)
