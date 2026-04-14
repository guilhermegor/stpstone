"""Brazilian sovereign bond pricing examples."""

from datetime import date

from stpstone.analytics.pricing.public_debt.br_national_bonds import BRSovereignPricer


cls_ = BRSovereignPricer()

print(f"LTN 1: {cls_.ltn(0.1297, 248, 1000.0, 252)}")
print(f"LTN 2: {cls_.ltn(0.1281, 748, 1000.0, 252)}")
print(f"LTN 3: {cls_.ltn(0.1350, 252, 1000.0, 252)}")
print(f"LTN 4: {cls_.ltn(0.1150, 252, 1000.0, 252)}")
print(f"NTN-F 1: {cls_.ntn_f(0.1298, [120, 248, 372, 499], 1000.0, 0.1)}")
print(f"NTN-F 2: {cls_.ntn_f(0.14, [122, 250, 374, 501, 625, 750, 874, 1000], 1000.0, 0.1)}")
print(f"NTN-F 3: {cls_.ntn_f(0.1298, [127, 251], 1000.0, 0.1)}")
print(f"NTN-B P 1: {cls_.ntn_b_principal(0.0613, 1089, 2494.977146, 0.0079, float(22 / 31))}")
print(f"NTN-B P 2: {cls_.ntn_b_principal(0.05, 837, 2736.989929, 0.005, float(22 / 31))}")
print(f"NTN-B 1: {cls_.ntn_b(0.061, [127, 250, 374, 500], 2508.949127, 0.0, 1.0)}")
print(f"NTN-B 2: {cls_.ntn_b(0.061, [127, 250], 2508.949127, 0.0970, 0.9951013)}")
print(f"LFT 1: {cls_.lft(0.0, 543, 6543.016794, 0.1175)}")
print(f"CUSTODY FEE 1: {cls_.custody_fee_bmfbov(2780.36, 180)}")
print(f"PR1 1: {cls_.pr1(date(2023, 6, 10), date(2023, 5, 15), date(2023, 6, 15))}")
print(f"PR1 2: {cls_.pr1(date(2023, 1, 10), date(2022, 12, 15), date(2023, 1, 15))}")

# # OUTPUT EXPECTED:
# LTN 1: 886.9
# LTN 2: 699.22
# LTN 3: 881.05
# LTN 4: 896.86
# NTN-F 1: 953.75
# NTN-F 2: 889.33
# NTN-F 3: 974.66
# NTN-B P 1: 1940.14
# NTN-B P 2: 2335.77
# NTN-B 1: 2506.65
# NTN-B 2: 2749.74
# LFT 1: 6545.9
# CUSTODY FEE 1: 4.11
# PR1 1: 0.8064516129032258
# PR1 2: 0.8387096774193549
