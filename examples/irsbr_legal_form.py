"""Brazilian IRS (Receita Federal) Legal Form (Naturezas) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_legal_form import IRSBRLegalForm


cls_ = IRSBRLegalForm(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR LEGAL FORM: \n{df_}")
df_.info()
