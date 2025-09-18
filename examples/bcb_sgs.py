from stpstone.ingestion.countries.br.macroeconomics.bcb_sgs import BCBSGS


cls_ = BCBSGS(
    list_series_codes=None,
    date_start=None,
    date_end=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF BCB SGS: \n{df_}")
df_.info()