class DataTester:
    def __init__(self):
        pass
    
    def test_max_pip(self, benchmark_times=None, benchmark_time=None):
        """Test if max pips have been correctly identified. 
        Given a list of max pips per time period per day, verify if the max pip 
        (max price) is indeed the largest in the given time period. 
        
        Methodology: if there exists a price that is larger than the max price, 
        the test fails.
        """
        # if benchmark_time==None and benchmark_times==None:
        #     print('Please provide an input for benchmark time. Test aborting...')
        # if benchmark_times:
        #     for btime in benchmark_times:
                
        # elif benchmark_time:

        