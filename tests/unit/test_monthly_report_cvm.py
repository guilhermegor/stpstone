"""Unit tests for stpstone.filings.br.monthly_report_cvm."""

from decimal import Decimal
from pathlib import Path

import pandas as pd
from pydantic import ValidationError
import pytest

from stpstone.filings.br._models_monthly_report_cvm import (
	ClientCount,
	DocumentHeader,
	OtcOperation,
	PatrimonyDistribution,
	PerfilMensalDocument,
	PerfilMensalRow,
	PerformanceFeeDetails,
)
from stpstone.filings.br.monthly_report_cvm import CvmMonthlyReport


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture()
def minimal_client_count() -> ClientCount:
	"""Return a minimal valid ClientCount fixture.

	Returns
	-------
	ClientCount
		Minimal ClientCount with all required fields set to non-negative values.
	"""
	return ClientCount(
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
	)


@pytest.fixture()
def minimal_row(minimal_client_count: ClientCount) -> PerfilMensalRow:
	"""Return a minimal valid PerfilMensalRow fixture.

	Parameters
	----------
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.

	Returns
	-------
	PerfilMensalRow
		Minimal PerfilMensalRow with only required fields set.
	"""
	return PerfilMensalRow(
		cnpj_fdo="12345678000195",
		nr_client=minimal_client_count,
		total_recurs_exter=Decimal("0.00"),
		total_recurs_br=Decimal("0.00"),
		tot_ativos_p_relac=Decimal("0.0"),
		tot_ativos_cred_priv=Decimal("0.0"),
	)


@pytest.fixture()
def minimal_header() -> DocumentHeader:
	"""Return a minimal valid DocumentHeader fixture.

	Returns
	-------
	DocumentHeader
		DocumentHeader with valid competency date and generation date.
	"""
	return DocumentHeader(dt_compt="03/2025", dt_gerac_arq="01/04/2025")


@pytest.fixture()
def minimal_doc(
	minimal_header: DocumentHeader,
	minimal_row: PerfilMensalRow,
) -> PerfilMensalDocument:
	"""Return a minimal valid PerfilMensalDocument fixture.

	Parameters
	----------
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.
	minimal_row : PerfilMensalRow
		Fixture providing a minimal PerfilMensalRow instance.

	Returns
	-------
	PerfilMensalDocument
		Document containing a single minimal row.
	"""
	return PerfilMensalDocument(header=minimal_header, rows=[minimal_row])


@pytest.fixture()
def reporter() -> CvmMonthlyReport:
	"""Return a CvmMonthlyReport instance.

	Returns
	-------
	CvmMonthlyReport
		CvmMonthlyReport instance under test.
	"""
	return CvmMonthlyReport()


# --------------------------
# Tests — DocumentHeader model
# --------------------------


def test_document_header_valid_formats() -> None:
	"""Verify DocumentHeader accepts valid MM/AAAA and DD/MM/AAAA date formats.

	Returns
	-------
	None
		No return value.
	"""
	h = DocumentHeader(dt_compt="03/2025", dt_gerac_arq="01/04/2025")
	assert h.dt_compt == "03/2025"
	assert h.dt_gerac_arq == "01/04/2025"
	assert h.cod_doc == 40
	assert h.versao == "4.0"


def test_document_header_invalid_dt_compt() -> None:
	"""Verify DocumentHeader rejects dt_compt not in MM/AAAA format.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(Exception, match="MM/AAAA"):
		DocumentHeader(dt_compt="2025-03", dt_gerac_arq="01/04/2025")


def test_document_header_invalid_dt_gerac_arq() -> None:
	"""Verify DocumentHeader rejects dt_gerac_arq not in DD/MM/AAAA format.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(Exception, match="DD/MM/AAAA"):
		DocumentHeader(dt_compt="03/2025", dt_gerac_arq="2025-04-01")


# --------------------------
# Tests — ClientCount model
# --------------------------


def test_client_count_rejects_negative() -> None:
	"""Verify ClientCount rejects negative investor counts.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(ValidationError):
		ClientCount(
			nr_pf_priv_bank=-1,
			nr_pf_varj=0,
			nr_pj_n_financ_priv_bank=0,
			nr_pj_n_financ_varj=0,
			nr_bnc_comerc=0,
			nr_pj_corr_dist=0,
			nr_pj_outr_financ=0,
			nr_inv_n_res=0,
			nr_ent_ab_prev_compl=0,
			nr_ent_fc_prev_compl=0,
			nr_reg_prev_serv_pub=0,
			nr_soc_seg_reseg=0,
			nr_soc_captlz_arrendm_merc=0,
			nr_fdos_club_inv=0,
			nr_cotst_distr_fdo=0,
			nr_outros_n_relac=0,
		)


# --------------------------
# Tests — PatrimonyDistribution model
# --------------------------


def test_patrimony_distribution_rejects_over_100() -> None:
	"""Verify PatrimonyDistribution rejects percentages above 100.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(ValidationError):
		PatrimonyDistribution(pr_pf_priv_bank=Decimal("100.1"))


def test_patrimony_distribution_accepts_zero_to_100() -> None:
	"""Verify PatrimonyDistribution accepts percentages in [0, 100].

	Returns
	-------
	None
		No return value.
	"""
	dp = PatrimonyDistribution(
		pr_pf_priv_bank=Decimal("0"),
		pr_pf_varj=Decimal("100"),
	)
	assert dp.pr_pf_priv_bank == Decimal("0")


# --------------------------
# Tests — PerfilMensalRow model
# --------------------------


def test_row_rejects_invalid_cnpj(minimal_client_count: ClientCount) -> None:
	"""Verify PerfilMensalRow rejects CNPJ strings not exactly 14 digits.

	Parameters
	----------
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(Exception, match="14 digits"):
		PerfilMensalRow(
			cnpj_fdo="1234",
			nr_client=minimal_client_count,
			total_recurs_exter=Decimal("0"),
			total_recurs_br=Decimal("0"),
			tot_ativos_p_relac=Decimal("0"),
			tot_ativos_cred_priv=Decimal("0"),
		)


def test_row_otc_list_limited_to_3(minimal_client_count: ClientCount) -> None:
	"""Verify PerfilMensalRow rejects OTC lists with more than 3 operations.

	Parameters
	----------
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.

	Returns
	-------
	None
		No return value.
	"""
	otc = OtcOperation(
		tp_pessoa="PJ",
		nr_pf_pj_comitente="12345678000195",
		parte_relacionada="N",
		valor_parte=Decimal("10.0"),
	)
	with pytest.raises(ValidationError):
		PerfilMensalRow(
			cnpj_fdo="12345678000195",
			nr_client=minimal_client_count,
			total_recurs_exter=Decimal("0"),
			total_recurs_br=Decimal("0"),
			tot_ativos_p_relac=Decimal("0"),
			tot_ativos_cred_priv=Decimal("0"),
			lista_oper_curs_merc_balcao=[otc, otc, otc, otc],
		)


# --------------------------
# Tests — OtcOperation model
# --------------------------


@pytest.mark.parametrize("doc_number", ["123", "1234567890123456", "ABCDEFGHIJKLMN"])
def test_otc_operation_rejects_invalid_doc(doc_number: str) -> None:
	"""Verify OtcOperation rejects document numbers with invalid length or format.

	Parameters
	----------
	doc_number : str
		Invalid document number to test rejection.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(ValidationError):
		OtcOperation(
			tp_pessoa="PJ",
			nr_pf_pj_comitente=doc_number,
			parte_relacionada="N",
			valor_parte=Decimal("10.0"),
		)


# --------------------------
# Tests — PerformanceFeeDetails model
# --------------------------


def test_performance_fee_details_rejects_bad_date() -> None:
	"""Verify PerformanceFeeDetails rejects date strings not in DD/MM/AAAA format.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(Exception, match="DD/MM/AAAA"):
		PerformanceFeeDetails(data_cota_fundo="2025-01-01", val_cota_fundo=Decimal("1.00"))


# --------------------------
# Tests — to_xml
# --------------------------


def test_to_xml_produces_string(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument
) -> None:
	"""Verify to_xml produces a string when no output_path is given.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.

	Returns
	-------
	None
		No return value.
	"""
	result = reporter.to_xml(minimal_doc)
	assert isinstance(result, str)


def test_to_xml_contains_required_tags(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument
) -> None:
	"""Verify to_xml output contains all mandatory CVM XML tags.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.

	Returns
	-------
	None
		No return value.
	"""
	xml = reporter.to_xml(minimal_doc)
	for tag in ["DOC_ARQ", "CAB_INFORM", "PERFIL_MENSAL", "ROW_PERFIL", "CNPJ_FDO"]:
		assert f"<{tag}>" in xml or f"<{tag} " in xml


def test_to_xml_uses_comma_decimal_separator(
	reporter: CvmMonthlyReport,
	minimal_client_count: ClientCount,
	minimal_header: DocumentHeader,
) -> None:
	"""Verify to_xml formats decimals with a comma separator as required by CVM.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.

	Returns
	-------
	None
		No return value.
	"""
	row = PerfilMensalRow(
		cnpj_fdo="12345678000195",
		nr_client=minimal_client_count,
		total_recurs_exter=Decimal("1234.56"),
		total_recurs_br=Decimal("0.00"),
		tot_ativos_p_relac=Decimal("0.0"),
		tot_ativos_cred_priv=Decimal("0.0"),
	)
	xml = reporter.to_xml(PerfilMensalDocument(header=minimal_header, rows=[row]))
	assert "1234,56" in xml
	assert "1234.56" not in xml


def test_to_xml_omits_none_tags(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument
) -> None:
	"""Verify to_xml omits optional blocks when the corresponding model field is None.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.

	Returns
	-------
	None
		No return value.
	"""
	xml = reporter.to_xml(minimal_doc)
	assert "DISTR_PATRIM" not in xml
	assert "RESM_TEOR_VT_PROFRD" not in xml


def test_to_xml_includes_namespace(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument
) -> None:
	"""Verify to_xml includes the urn:perf namespace declaration.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.

	Returns
	-------
	None
		No return value.
	"""
	xml = reporter.to_xml(minimal_doc)
	assert 'xmlns="urn:perf"' in xml


def test_to_xml_writes_file(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument, tmp_path: Path
) -> None:
	"""Verify to_xml writes a windows-1252 encoded file when output_path is provided.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	out = str(tmp_path / "out.xml")
	result = reporter.to_xml(minimal_doc, output_path=out)
	assert result is None
	assert Path(out).exists()
	content = Path(out).read_text(encoding="windows-1252")
	assert "DOC_ARQ" in content


def test_to_xml_escapes_special_chars(
	reporter: CvmMonthlyReport,
	minimal_client_count: ClientCount,
	minimal_header: DocumentHeader,
) -> None:
	"""Verify to_xml escapes XML special characters in text content.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.

	Returns
	-------
	None
		No return value.
	"""
	row = PerfilMensalRow(
		cnpj_fdo="12345678000195",
		nr_client=minimal_client_count,
		total_recurs_exter=Decimal("0"),
		total_recurs_br=Decimal("0"),
		tot_ativos_p_relac=Decimal("0"),
		tot_ativos_cred_priv=Decimal("0"),
		inf_compl_perfil="Ativo com lucro > 0 & retorno <esperado>",
	)
	xml = reporter.to_xml(PerfilMensalDocument(header=minimal_header, rows=[row]))
	assert "&amp;" in xml or "&gt;" in xml or "&lt;" in xml


# --------------------------
# Tests — to_csv / to_excel
# --------------------------


def test_to_csv_creates_file(
	reporter: CvmMonthlyReport, minimal_row: PerfilMensalRow, tmp_path: Path
) -> None:
	"""Verify to_csv creates a readable CSV file with one row per record.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_row : PerfilMensalRow
		Fixture providing a minimal PerfilMensalRow instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	out = str(tmp_path / "out.csv")
	reporter.to_csv([minimal_row], out)
	assert Path(out).exists()
	df_ = pd.read_csv(out)
	assert len(df_) == 1


def test_to_csv_accepts_dicts(reporter: CvmMonthlyReport, tmp_path: Path) -> None:
	"""Verify to_csv accepts a list of plain dicts as input.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	out = str(tmp_path / "out.csv")
	reporter.to_csv([{"a": 1, "b": "hello"}], out)
	df_ = pd.read_csv(out)
	assert "a" in df_.columns


def test_to_csv_accepts_dataframe(reporter: CvmMonthlyReport, tmp_path: Path) -> None:
	"""Verify to_csv accepts a DataFrame as input.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	out = str(tmp_path / "out.csv")
	reporter.to_csv(pd.DataFrame([{"x": 1}]), out)
	df_ = pd.read_csv(out)
	assert "x" in df_.columns


def test_to_csv_raises_on_unsupported_type(reporter: CvmMonthlyReport, tmp_path: Path) -> None:
	"""Verify to_csv raises TypeError for unsupported input types.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(TypeError):
		reporter.to_csv("not_a_list", str(tmp_path / "out.csv"))  # type: ignore[arg-type]


def test_to_excel_creates_file(
	reporter: CvmMonthlyReport, minimal_row: PerfilMensalRow, tmp_path: Path
) -> None:
	"""Verify to_excel creates a readable xlsx file with one row per record.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_row : PerfilMensalRow
		Fixture providing a minimal PerfilMensalRow instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	out = str(tmp_path / "out.xlsx")
	reporter.to_excel([minimal_row], out)
	assert Path(out).exists()
	df_ = pd.read_excel(out)
	assert len(df_) == 1


# --------------------------
# Tests — from_xml
# --------------------------


def test_from_xml_produces_dataframe(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument, tmp_path: Path
) -> None:
	"""Verify from_xml produces a DataFrame with one row per ROW_PERFIL block.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	xml_path = str(tmp_path / "doc.xml")
	reporter.to_xml(minimal_doc, output_path=xml_path)
	df_ = reporter.from_xml(xml_path)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1


def test_from_xml_preserves_cnpj(
	reporter: CvmMonthlyReport, minimal_doc: PerfilMensalDocument, tmp_path: Path
) -> None:
	"""Verify from_xml preserves the CNPJ value from the XML source.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	xml_path = str(tmp_path / "doc.xml")
	reporter.to_xml(minimal_doc, output_path=xml_path)
	df_ = reporter.from_xml(xml_path)
	assert df_["cnpj_fdo"].iloc[0] == "12345678000195"


def test_from_xml_raises_on_invalid_file(reporter: CvmMonthlyReport, tmp_path: Path) -> None:
	"""Verify from_xml raises ValueError when the file cannot be parsed as XML.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	bad = tmp_path / "bad.xml"
	bad.write_text("this is not xml", encoding="utf-8")
	with pytest.raises(ValueError):
		reporter.from_xml(str(bad))


# --------------------------
# Tests — from_csv_excel
# --------------------------


def test_from_csv_excel_round_trip(
	reporter: CvmMonthlyReport,
	minimal_doc: PerfilMensalDocument,
	minimal_header: DocumentHeader,
	tmp_path: Path,
) -> None:
	"""Verify CSV to XML round-trip preserves the original CNPJ.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	xml_path = str(tmp_path / "doc.xml")
	csv_path = str(tmp_path / "doc.csv")
	reporter.to_xml(minimal_doc, output_path=xml_path)
	df_ = reporter.from_xml(xml_path)
	reporter.to_csv(df_, csv_path)
	rebuilt_xml = reporter.from_csv_excel(csv_path, header=minimal_header)
	assert isinstance(rebuilt_xml, str)
	assert "12345678000195" in rebuilt_xml


def test_from_csv_excel_writes_file_when_output_path_given(
	reporter: CvmMonthlyReport,
	minimal_doc: PerfilMensalDocument,
	minimal_header: DocumentHeader,
	tmp_path: Path,
) -> None:
	"""Verify from_csv_excel writes an XML file when output_path is provided.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_doc : PerfilMensalDocument
		Fixture providing a minimal PerfilMensalDocument instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	xml_path = str(tmp_path / "doc.xml")
	csv_path = str(tmp_path / "doc.csv")
	out_xml = str(tmp_path / "rebuilt.xml")
	reporter.to_xml(minimal_doc, output_path=xml_path)
	reporter.to_csv(reporter.from_xml(xml_path), csv_path)
	result = reporter.from_csv_excel(csv_path, header=minimal_header, output_path=out_xml)
	assert result is None
	assert Path(out_xml).exists()


def test_from_csv_excel_raises_on_unsupported_extension(
	reporter: CvmMonthlyReport, minimal_header: DocumentHeader, tmp_path: Path
) -> None:
	"""Verify from_csv_excel raises ValueError for unsupported file extensions.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.
	tmp_path : Path
		Temporary directory provided by pytest.

	Returns
	-------
	None
		No return value.
	"""
	bad = tmp_path / "data.parquet"
	bad.write_bytes(b"fake")
	with pytest.raises(ValueError, match="Unsupported"):
		reporter.from_csv_excel(str(bad), header=minimal_header)


# --------------------------
# Tests — _fmt_decimal
# --------------------------


@pytest.mark.parametrize(
	"value,places,expected",
	[
		(Decimal("12.34"), 2, "12,34"),
		(Decimal("0"), 1, "0,0"),
		(Decimal("100"), 1, "100,0"),
		(Decimal("0.00015"), 4, "0,0002"),
	],
)
def test_fmt_decimal(value: Decimal, places: int, expected: str) -> None:
	"""Verify _fmt_decimal formats decimals with a comma and correct decimal places.

	Parameters
	----------
	value : Decimal
		Input decimal value to format.
	places : int
		Number of decimal places to apply.
	expected : str
		Expected comma-formatted string output.

	Returns
	-------
	None
		No return value.
	"""
	assert CvmMonthlyReport._fmt_decimal(value, places) == expected


# --------------------------
# Tests — _parse_decimal
# --------------------------


@pytest.mark.parametrize(
	"raw,expected",
	[
		("12,34", Decimal("12.34")),
		("0,0", Decimal("0.0")),
		("100,0", Decimal("100.0")),
	],
)
def test_parse_decimal(raw: str, expected: Decimal) -> None:
	"""Verify _parse_decimal converts comma-separated strings to Decimal.

	Parameters
	----------
	raw : str
		Comma-formatted decimal string to parse.
	expected : Decimal
		Expected Decimal value after parsing.

	Returns
	-------
	None
		No return value.
	"""
	assert CvmMonthlyReport._parse_decimal(raw) == expected


def test_parse_decimal_raises_on_non_numeric() -> None:
	"""Verify _parse_decimal raises ValueError for non-numeric input.

	Returns
	-------
	None
		No return value.
	"""
	with pytest.raises(ValueError):
		CvmMonthlyReport._parse_decimal("abc")


# --------------------------
# Tests — full optional blocks in XML
# --------------------------


def test_to_xml_includes_distr_patrim_when_set(
	reporter: CvmMonthlyReport,
	minimal_client_count: ClientCount,
	minimal_header: DocumentHeader,
) -> None:
	"""Verify to_xml includes the DISTR_PATRIM block when the field is set.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.

	Returns
	-------
	None
		No return value.
	"""
	row = PerfilMensalRow(
		cnpj_fdo="12345678000195",
		nr_client=minimal_client_count,
		distr_patrim=PatrimonyDistribution(pr_pf_varj=Decimal("100.0")),
		total_recurs_exter=Decimal("0"),
		total_recurs_br=Decimal("0"),
		tot_ativos_p_relac=Decimal("0"),
		tot_ativos_cred_priv=Decimal("0"),
	)
	xml = reporter.to_xml(PerfilMensalDocument(header=minimal_header, rows=[row]))
	assert "<DISTR_PATRIM>" in xml
	assert "<PR_PF_VARJ>100,0</PR_PF_VARJ>" in xml


def test_to_xml_includes_otc_block(
	reporter: CvmMonthlyReport,
	minimal_client_count: ClientCount,
	minimal_header: DocumentHeader,
) -> None:
	"""Verify to_xml includes the OTC list block when lista_oper_curs_merc_balcao is set.

	Parameters
	----------
	reporter : CvmMonthlyReport
		CvmMonthlyReport instance under test.
	minimal_client_count : ClientCount
		Fixture providing a minimal ClientCount instance.
	minimal_header : DocumentHeader
		Fixture providing a minimal DocumentHeader instance.

	Returns
	-------
	None
		No return value.
	"""
	row = PerfilMensalRow(
		cnpj_fdo="12345678000195",
		nr_client=minimal_client_count,
		total_recurs_exter=Decimal("0"),
		total_recurs_br=Decimal("0"),
		tot_ativos_p_relac=Decimal("0"),
		tot_ativos_cred_priv=Decimal("0"),
		lista_oper_curs_merc_balcao=[
			OtcOperation(
				tp_pessoa="PJ",
				nr_pf_pj_comitente="12345678000195",
				parte_relacionada="N",
				valor_parte=Decimal("10.5"),
			)
		],
	)
	xml = reporter.to_xml(PerfilMensalDocument(header=minimal_header, rows=[row]))
	assert "<LISTA_OPER_CURS_MERC_BALCAO>" in xml
	assert "<TP_PESSOA>PJ</TP_PESSOA>" in xml
	assert "<VALOR_PARTE>10,5</VALOR_PARTE>" in xml
