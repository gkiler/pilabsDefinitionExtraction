import os
import time

begin = time.time()
os.system("py clean.py")
os.system("py format.py")
os.system("py ParallelGraphApply.py")
os.system("py UsesToGraph.py")
end = time.time()

print("Total process time: " + str(end - begin) + "s")