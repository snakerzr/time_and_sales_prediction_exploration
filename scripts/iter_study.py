import pandas as pd

df_test = pd.DataFrame([1,2,3])

counter = 0

def return_df():
    global counter
    df = pd.DataFrame([1,2,3])
    if counter < 3:
        counter += 1
        return df
    else:
        return 'word'


class iter_:
    
    def __init__(self,function, sentinel_type):
        self.function = function
        self.sentinel_type = sentinel_type
        
    def __iter__(self):
        return self
    
    def __next__(self):
        result = self.function()
        if type(result) == self.sentinel_type:
            raise StopIteration
        return result
            
        
for x in iter_(return_df,pd.core.frame.DataFrame):
    print(x)
    
for x in iter_(return_df,str):
    print(x)