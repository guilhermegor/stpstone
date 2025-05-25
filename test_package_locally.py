from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.analytics.pricing.derivatives.european_options import EuropeanOptions


print("*** Dates BR Tester ***")
print(f"Current Date: {DatesBR().curr_date}")
print(f"3 working days before current date: {DatesBR().sub_working_days(DatesBR().curr_date, 3)}")
print(f"3 working days after current date: {DatesBR().add_working_days(DatesBR().curr_date, 3)}")

print("\n*** Unix Timestamp Tester ***")
print(f"Current Unix Timestamp: {DatesBR().datetime_to_unix_timestamp(DatesBR().curr_date)}")

print("\n*** European Options Tester ***")
print(f"Black Scholes Call Option: {EuropeanOptions().general_opt_price(103.0, 100.0, 0.025, 0.25, 0.2, 0.0, 0.0, 'call')}")
print(f"Black Scholes Put Option: {EuropeanOptions().general_opt_price(103.0, 100.0, 0.025, 0.25, 0.2, 0.0, 0.0, 'put')}")