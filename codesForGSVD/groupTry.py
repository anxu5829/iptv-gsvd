import pandas as pd

data = pd.DataFrame({"hello":[0,0,1,1],"hi":[2,3,4,5]})
g = data.groupby(data.hello)
g.apply(lambda x: (x['hello'].tolist())[0])