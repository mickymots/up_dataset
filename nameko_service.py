from nameko.rpc import rpc
from max_minute_vol_parallel import build_dataset

class GreetingService:
    name = "greeting_service"

    @rpc
    def hello(self, name):
        
        return "Hello, {}!".format(name)


    @rpc
    def get_dataset(self, ticker):
        
        return build_dataset(symbol=ticker)

