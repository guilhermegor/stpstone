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
	PerformanceFeeDetails,
	PrimitiveRiskFactor,
	PrivateCreditIssuer,
	VarOutros,
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
		# All pr_* fields represent % of AUM by investor segment; must sum to 100.
		pr_pf_priv_bank=Decimal("5.0"),           # private-banking individuals
		pr_pf_varj=Decimal("65.0"),               # retail individuals
		pr_pj_n_financ_priv_bank=Decimal("0.0"),  # non-financial PJ, private-banking
		pr_pj_n_financ_varj=Decimal("10.0"),      # non-financial PJ, retail
		pr_bnc_comerc=Decimal("0.0"),             # commercial banks
		pr_pj_corr_dist=Decimal("5.0"),           # broker-dealers / distributors
		pr_pj_outr_financ=Decimal("0.0"),         # other financial PJ
		pr_inv_n_res=Decimal("2.0"),              # non-resident investors
		pr_ent_ab_prev_compl=Decimal("3.0"),      # open complementary pension funds
		pr_ent_fc_prev_compl=Decimal("0.0"),      # closed complementary pension funds
		pr_reg_prev_serv_pub=Decimal("0.0"),      # public-servant pension schemes
		pr_soc_seg_reseg=Decimal("0.0"),          # insurance / reinsurance companies
		pr_soc_captlz_arrendm_merc=Decimal("0.0"),# capitalization / leasing companies
		pr_fdos_club_inv=Decimal("5.0"),          # investment clubs / other funds
		pr_cotst_distr_fdo=Decimal("0.0"),        # fund-of-fund distributors
		pr_outros_n_relac=Decimal("5.0"),         # unrelated others
	),
	# Summary of vote justification used in shareholder meetings (optional, max 4 000 chars).
	resm_teor_vt_profrd="Votação favorável à reeleição do conselho fiscal conforme política ESG.",
	# Justification for deviating from the voting policy summary (optional, max 4 000 chars).
	just_sum_vt_profrd="Desvio da política padrão em razão de conflito de interesse identificado.",
	# VaR as a % of NAV under the chosen model (MOD_VAR_UTILIZ).
	var_perc_pl=Decimal("0.0123"),
	# VaR model: 1=parametric, 2=historical simulation, 3=Monte Carlo.
	mod_var_utiliz="1",
	# Portfolio weighted-average maturity in business days.
	praz_med_cart_tit=Decimal("24.0000"),
	# Deliberation result from the last general meeting (optional, max 4 000 chars).
	res_delib="Aprovada a distribuição de rendimentos conforme proposta da gestão.",
	# Total foreign-currency inflows/outflows (mandatory).
	total_recurs_exter=Decimal("0.00"),
	# Total BRL-denominated inflows/outflows (mandatory).
	total_recurs_br=Decimal("0.00"),
	# Worst-case stress scenario: % change in NAV and list of contributing risk factors.
	variacao_perc_val_cota=VarPercValCota(
		val_percent=Decimal("1.50"),
		lista_fator_primit_risco=[
			PrimitiveRiskFactor(
				nome_fator_primit_risco="IBOVESPA",
				cen_util="Cenário de queda 20%",
			)
		],
	),
	# Daily worst-case % change in NAV under the stress scenario above.
	var_diar_perc_cota_fdo_pior_cen_estress=Decimal("-2.50"),
	# DV01-style sensitivity: daily % NAV change per 1 pp shift in annual interest rates.
	var_diar_perc_patrim_fdo_var_n_taxa_anual=Decimal("-0.0150"),
	# Daily % NAV change per 1% move in FX rates.
	var_diar_perc_patrim_fdo_var_n_taxa_cambio=Decimal("0.0080"),
	# Daily % NAV change per 1% move in equity prices.
	var_patrim_fdo_n_preco_acoes=Decimal("0.9000"),
	# Sensitivity to a non-standard risk factor not covered by the four fields above.
	variacao_diar_perc_patrim_fdo_var_n_outros=VarOutros(
		fator_risco_outros="Spread de crédito privado (debêntures high yield)",
		val_percent_outros=Decimal("0.0320"),
	),
	# OTC derivatives notional exposure block (optional, up to 5 risk factors).
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
	# Top-3 OTC counterparties without central clearing (optional, max 3 entries).
	lista_oper_curs_merc_balcao=[
		OtcOperation(
			tp_pessoa="PJ",
			nr_pf_pj_comitente="12345678000195",
			parte_relacionada="N",
			valor_parte=Decimal("15.5"),
		)
	],
	# Total related-party assets as % of NAV (mandatory).
	tot_ativos_p_relac=Decimal("0.0"),
	# Top-3 private credit issuers held by the fund (optional, max 3 entries).
	lista_emissores_tit_cred_priv=[
		PrivateCreditIssuer(
			tp_pessoa_emissor="PJ",
			nr_pf_pj_emissor="98765432000188",
			parte_relacionada="N",
			valor_parte=Decimal("8.3"),
		)
	],
	# Total private credit assets as % of NAV (mandatory).
	tot_ativos_cred_priv=Decimal("0.0"),
	# "S" = performance fee prohibited by regulation; "N" = not prohibited (optional).
	ved_regul_cobr_taxa_perform="N",
	# When ved_regul_cobr_taxa_perform == "S": last performance-fee event details.
	resp_ved_regul_cobr_taxa_perform=None,
	# Distribution amount paid to quota-holders in the reference month (optional).
	montante_distrib=Decimal("125000.00"),
	# Free-text complementary information for CVM (optional, max 500 chars).
	inf_compl_perfil="Fundo enquadrado como Fundo de Investimento em Renda Fixa Simples.",
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
