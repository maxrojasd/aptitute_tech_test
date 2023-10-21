import sys
import os
curr_dir = os.path.dirname(os.path.abspath(__file__))
utils_folder = os.path.abspath(os.path.join(curr_dir, '..'))
sys.path.insert(1, utils_folder)
from utils.func_utils import *

# Unit Test per function
## get_timeframe
def get_timeframe_date_unit_test():
    init_year, init_month = 2022, 3
    final_year, final_month = 2022, 5
    
    expected_output = {
        2022: ['03', '04', '05']
    }
    
    result = get_timeframe(init_year, init_month, final_year, final_month)
    
    assert result == expected_output, f"Test was a failure: It expected {expected_output}, but it got {result}"
    print('Test was a success')

# Main function
if __name__ == '__main__':
    # Run first Unit test for get_timeframe_date()
    try:
        get_timeframe_date_unit_test()
        print(f'Successfully ran get_timeframe_date_unit_test unit test')

    except Exception as e:
        print(f'Unit test get_timeframe_date_unit_test failed. Traceback below: {str(e)}')

    # Add more tests accordingly below..