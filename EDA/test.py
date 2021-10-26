import src.oasis_api as oa

test_pge = oa.get_LMP('20211001', '20211010')

print(test_pge.head())
print(test_pge.dtypes)
