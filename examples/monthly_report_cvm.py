"""CVM Monthly Profile (Perfil Mensal) serialization and format conversion."""

from decimal import Decimal
from pathlib import Path
import tempfile

from stpstone.filings.br._models_monthly_report_cvm import (
	ClientCount,
	DocumentHeader,
	NominalRiskBlock,
	NominalRiskFactor,
	OtcOperation,
	PatrimonyDistribution,
	PerfilMensalDocument,
	PerfilMensalRow,
	PrimitiveRiskFactor,
	VarPercValCota,
)
from stpstone.filings.br.monthly_report_cvm import CvmMonthlyReport


# ----- build a minimal document -----

header = DocumentHeader(dt_compt="03/2025", dt_gerac_arq="01/04/2025")

row = PerfilMensalRow(
	cnpj_fdo="12345678000195",
	nr_client=ClientCount(
		nr_pf_priv_bank=10,
		nr_pf_varj=200,
		nr_pj_n_financ_priv_bank=0,
		nr_pj_n_financ_varj=5,
		nr_bnc_comerc=1,
		nr_pj_corr_dist=2,
		nr_pj_outr_financ=0,
		nr_inv_n_res=0,
		nr_ent_ab_prev_compl=0,
		nr_ent_fc_prev_compl=0,
		nr_reg_prev_serv_pub=0,
		nr_soc_seg_reseg=0,
		nr_soc_captlz_arrendm_merc=0,
		nr_fdos_club_inv=0,
		nr_cotst_distr_fdo=0,
		nr_outros_n_relac=3,
	),
	distr_patrim=PatrimonyDistribution(
		pr_pf_priv_bank=Decimal("5.0"),
		pr_pf_varj=Decimal("90.0"),
		pr_outros_n_relac=Decimal("5.0"),
	),
	var_perc_pl=Decimal("0.0123"),
	mod_var_utiliz="1",
	praz_med_cart_tit=Decimal("24.0000"),
	total_recurs_exter=Decimal("0.00"),
	total_recurs_br=Decimal("0.00"),
	variacao_perc_val_cota=VarPercValCota(
		val_percent=Decimal("1.50"),
		lista_fator_primit_risco=[
			PrimitiveRiskFactor(
				nome_fator_primit_risco="IBOVESPA",
				cen_util="Cenário de queda 20%",
			)
		],
	),
	valor_noc_tot_contrat_deriv_mant_fdo=NominalRiskBlock(
		val_colateral=Decimal("10.00"),
		lista_fator_risco_noc=[
			NominalRiskFactor(
				nome_fator_noc="JUROS-PRE",
				val_fator_risco_noc_long=500,
				val_fator_risco_noc_short=0,
			)
		],
	),
	lista_oper_curs_merc_balcao=[
		OtcOperation(
			tp_pessoa="PJ",
			nr_pf_pj_comitente="12345678000195",
			parte_relacionada="N",
			valor_parte=Decimal("15.5"),
		)
	],
	tot_ativos_p_relac=Decimal("0.0"),
	tot_ativos_cred_priv=Decimal("0.0"),
)

doc = PerfilMensalDocument(header=header, rows=[row])
cls_ = CvmMonthlyReport()

tmp_dir = Path(tempfile.gettempdir())
xml_path = tmp_dir / "perfil_mensal.xml"
csv_path = tmp_dir / "perfil_mensal.csv"
xlsx_path = tmp_dir / "perfil_mensal.xlsx"

# 1) serialize to XML string
xml_str = cls_.to_xml(doc)
print("=== XML OUTPUT (first 800 chars) ===")
print(xml_str[:800])

# 2) serialize to CSV (agnostic — also works with any list[BaseModel] or list[dict])
cls_.to_csv([row], csv_path)
print(f"\n=== CSV written to {csv_path} ===")

# 3) serialize to Excel
cls_.to_excel([row], xlsx_path)
print(f"=== Excel written to {xlsx_path} ===")

# 4) round-trip: save XML → read back → DataFrame
cls_.to_xml(doc, output_path=xml_path)
df_ = cls_.from_xml(xml_path)
print("\n=== DataFrame from XML ===")
print(df_)
df_.info()

# 5) round-trip: DataFrame (CSV) → XML
xml_from_csv = cls_.from_csv_excel(csv_path, header=header)
print("\n=== XML rebuilt from CSV (first 400 chars) ===")
print(xml_from_csv[:400])
