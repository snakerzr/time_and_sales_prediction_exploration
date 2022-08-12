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