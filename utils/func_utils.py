
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Functions
def get_input_date(timeframe):
    while True:
        input_date = input('(MM-YYYY): ')
        try:
            # Transform date to page source format
            # Split date
            trans_date = datetime.strptime(input_date, '%m-%Y')
            year = trans_date.year
            # Get month as text
            month_str = trans_date.strftime('%B')
            # Get month as number
            month_int = int(trans_date.strftime('%m'))
            print(f'{timeframe} date is {month_str} of {year}')
            return year, month_int
        except ValueError:
            print('Date in incorrect format, please try again with format (MM-YYYY): ')
            continue

# Function to create timeframe for the period selected
def get_timeframe(init_year, init_month,final_year, final_month):
    timeframe = {}
    # Create datatime objects
    init_date = datetime(init_year, init_month, 1)
    final_date = datetime(final_year, final_month, 1)

    # Generate the timeframe elements
    while init_date <= final_date:
        year = init_date.year
        month = init_date.strftime('%m')

        # Add the elements to the corresponding dict
        if year in timeframe:
            timeframe[year].append(month)
        else:
            timeframe[year] = [month]

        # Monthly counter
        init_date += relativedelta(months=1)

    return timeframe
