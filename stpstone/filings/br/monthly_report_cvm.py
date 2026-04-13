"""CVM Monthly Profile (Perfil Mensal) report serializer, deserializer and format converter."""

import json
import xml.sax.saxutils as saxutils
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
from defusedxml.ElementTree import fromstring
from pydantic import BaseModel

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


# List fields that are JSON-encoded strings in the flat CSV/Excel format.
_LIST_FIELDS = frozenset({
    "lista_oper_curs_merc_balcao",
    "lista_emissores_tit_cred_priv",
    "variacao_perc_val_cota.lista_fator_primit_risco",
    "valor_noc_tot_contrat_deriv_mant_fdo.lista_fator_risco_noc",
})


class CvmMonthlyReport:
    """Serialize, deserialize and convert CVM Monthly Profile (Perfil Mensal) reports.

    Methods
    -------
    to_xml(doc, output_path)
        Serialize a validated document to a CVM-compliant XML string.
    to_csv(records, output_path)
        Write any list of Pydantic models or dicts to a CSV file.
    to_excel(records, output_path)
        Write any list of Pydantic models or dicts to an Excel file.
    from_xml(path)
        Parse a CVM XML file into a flattened DataFrame.
    from_csv_excel(path, header, output_path)
        Read a flat CSV/Excel produced by from_xml and convert back to CVM XML.
    """

    # ------------------------------------------------------------------
    # Public serialization
    # ------------------------------------------------------------------

    def to_xml(
        self,
        doc: PerfilMensalDocument,
        output_path: Optional[str] = None,
        versao: str = "4.0",
    ) -> Optional[str]:
        """Serialize a validated PerfilMensalDocument to a CVM-compliant XML string.

        Produces UTF-8 text internally; when output_path is given the file is
        written with windows-1252 encoding as required by CVM.

        Parameters
        ----------
        doc : PerfilMensalDocument
            Fully validated document model.
        output_path : Optional[str], optional
            Destination file path. If provided, the file is written and None is
            returned; otherwise the XML string is returned, by default None.
        versao : str, optional
            CVM document format version placed in the VERSAO tag, by default "4.0".

        Returns
        -------
        Optional[str]
            XML string when output_path is None, else None.
        """
        xml_str = self._build_xml_str(doc, versao=versao)
        if output_path is not None:
            Path(output_path).write_text(xml_str, encoding="windows-1252")
            return None
        return xml_str

    def to_csv(
        self,
        records: Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame],
        output_path: str,
    ) -> None:
        """Write records to a CSV file.

        Accepts any list of Pydantic models, dicts, or a DataFrame. Nested
        objects are flattened with dot-separated column names; list fields are
        JSON-encoded strings.

        Parameters
        ----------
        records : Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame]
            Source records.
        output_path : str
            Destination CSV file path.

        Returns
        -------
        None
        """
        df = self._records_to_df(records)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

    def to_excel(
        self,
        records: Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame],
        output_path: str,
    ) -> None:
        """Write records to an Excel (.xlsx) file.

        Accepts any list of Pydantic models, dicts, or a DataFrame. Nested
        objects are flattened with dot-separated column names; list fields are
        JSON-encoded strings.

        Parameters
        ----------
        records : Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame]
            Source records.
        output_path : str
            Destination .xlsx file path.

        Returns
        -------
        None
        """
        df = self._records_to_df(records)
        df.to_excel(output_path, index=False)

    # ------------------------------------------------------------------
    # Public deserialization / conversion
    # ------------------------------------------------------------------

    def from_xml(self, path: str) -> pd.DataFrame:
        """Parse a CVM Perfil Mensal XML file into a flattened DataFrame.

        Each row in the returned DataFrame corresponds to one ROW_PERFIL block.
        Scalar and nested-scalar fields are flattened with dot-separated names.
        List fields are stored as JSON strings. The DataFrame can be passed
        directly to to_csv or to_excel.

        Parameters
        ----------
        path : str
            Path to the XML file (windows-1252 or UTF-8 encoded).

        Returns
        -------
        pd.DataFrame
            Flattened DataFrame with one row per fund class.

        Raises
        ------
        ValueError
            If the file cannot be parsed or does not match the expected schema.
        """
        doc = self._parse_xml_file(path)
        return pd.DataFrame([self._row_to_flat_dict(row) for row in doc.rows])

    def from_csv_excel(
        self,
        path: str,
        header: DocumentHeader,
        output_path: Optional[str] = None,
    ) -> Optional[str]:
        """Convert a flat CSV or Excel file (produced by from_xml) back to CVM XML.

        Parameters
        ----------
        path : str
            Path to the source CSV (.csv) or Excel (.xlsx / .xls) file.
        header : DocumentHeader
            Document-level header (competency date, generation date, version).
        output_path : Optional[str], optional
            Destination XML file path. If given, writes the file (windows-1252)
            and returns None; otherwise returns the XML string, by default None.

        Returns
        -------
        Optional[str]
            XML string when output_path is None, else None.

        Raises
        ------
        ValueError
            If the file format is not recognised or a required column is missing.
        """
        df = self._read_tabular(path)
        rows = [self._flat_dict_to_row(row_dict) for row_dict in df.to_dict(orient="records")]
        doc = PerfilMensalDocument(header=header, rows=rows)
        return self.to_xml(doc, output_path=output_path)

    # ------------------------------------------------------------------
    # XML building helpers (top-down call order)
    # ------------------------------------------------------------------

    def _build_xml_str(self, doc: PerfilMensalDocument, versao: str = "4.0") -> str:
        """Build the full CVM XML string from a PerfilMensalDocument.

        Parameters
        ----------
        doc : PerfilMensalDocument
            Validated document model.
        versao : str, optional
            CVM document format version, by default "4.0".

        Returns
        -------
        str
            CVM-compliant XML string (UTF-8 in memory).
        """
        lines: list[str] = []
        lines.append('<?xml version="1.0" encoding="windows-1252"?>')
        lines.append('<DOC_ARQ xmlns="urn:perf">')
        lines.append("    <CAB_INFORM>")
        lines.append(f"        <COD_DOC>{doc.header.cod_doc}</COD_DOC>")
        lines.append(f"        <DT_COMPT>{doc.header.dt_compt}</DT_COMPT>")
        lines.append(f"        <DT_GERAC_ARQ>{doc.header.dt_gerac_arq}</DT_GERAC_ARQ>")
        lines.append(f"        <VERSAO>{versao}</VERSAO>")
        lines.append("    </CAB_INFORM>")
        lines.append("    <PERFIL_MENSAL>")
        for row in doc.rows:
            lines.extend(self._build_row_lines(row))
        lines.append("    </PERFIL_MENSAL>")
        lines.append("</DOC_ARQ>")
        return "\n".join(lines)

    def _build_row_lines(self, row: PerfilMensalRow) -> list[str]:
        """Build the XML lines for one ROW_PERFIL block.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.

        Returns
        -------
        list[str]
            Ordered XML lines for this row.
        """
        ind = "        "  # 8-space base indent inside PERFIL_MENSAL
        lines: list[str] = [
            f"{ind}<ROW_PERFIL>",
            f"{ind}    <CNPJ_FDO>{row.cnpj_fdo}</CNPJ_FDO>",
        ]
        lines.extend(self._build_nr_client_lines(row.nr_client, ind))
        lines.extend(self._build_distr_patrim_lines(row.distr_patrim, ind))
        lines.extend(self._build_risk_scalar_lines(row, ind))
        lines.extend(self._build_usd_flow_lines(row, ind))
        if row.variacao_perc_val_cota is not None:
            lines.extend(self._build_variacao_vpc_lines(row.variacao_perc_val_cota, ind))
        lines.extend(self._build_sensitivity_lines(row, ind))
        if row.variacao_diar_perc_patrim_fdo_var_n_outros is not None:
            lines.extend(
                self._build_var_outros_lines(
                    row.variacao_diar_perc_patrim_fdo_var_n_outros, ind
                )
            )
        if row.valor_noc_tot_contrat_deriv_mant_fdo is not None:
            lines.extend(
                self._build_nominal_risk_lines(
                    row.valor_noc_tot_contrat_deriv_mant_fdo, ind
                )
            )
        lines.extend(self._build_otc_lines(row.lista_oper_curs_merc_balcao, ind))
        lines.append(
            f"{ind}    <TOT_ATIVOS_P_RELAC>"
            f"{self._fmt_decimal(row.tot_ativos_p_relac, 1)}"
            f"</TOT_ATIVOS_P_RELAC>"
        )
        lines.extend(self._build_issuers_lines(row.lista_emissores_tit_cred_priv, ind))
        lines.append(
            f"{ind}    <TOT_ATIVOS_CRED_PRIV>"
            f"{self._fmt_decimal(row.tot_ativos_cred_priv, 1)}"
            f"</TOT_ATIVOS_CRED_PRIV>"
        )
        lines.extend(self._build_performance_fee_lines(row, ind))
        if row.montante_distrib is not None:
            lines.append(
                f"{ind}    <MONTANTE_DISTRIB>"
                f"{self._fmt_decimal(row.montante_distrib, 2)}"
                f"</MONTANTE_DISTRIB>"
            )
        if row.inf_compl_perfil is not None:
            lines.append(
                f"{ind}    <INF_COMPL_PERFIL>"
                f"{saxutils.escape(row.inf_compl_perfil[:500])}"
                f"</INF_COMPL_PERFIL>"
            )
        lines.append(f"{ind}</ROW_PERFIL>")
        return lines

    def _build_nr_client_lines(self, c: ClientCount, ind: str) -> list[str]:
        """Build XML lines for the NR_CLIENT block.

        Parameters
        ----------
        c : ClientCount
            Client count model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for the NR_CLIENT block.
        """
        inner = f"{ind}        "
        pairs = [
            ("NR_PF_PRIV_BANK", c.nr_pf_priv_bank),
            ("NR_PF_VARJ", c.nr_pf_varj),
            ("NR_PJ_N_FINANC_PRIV_BANK", c.nr_pj_n_financ_priv_bank),
            ("NR_PJ_N_FINANC_VARJ", c.nr_pj_n_financ_varj),
            ("NR_BNC_COMERC", c.nr_bnc_comerc),
            ("NR_PJ_CORR_DIST", c.nr_pj_corr_dist),
            ("NR_PJ_OUTR_FINANC", c.nr_pj_outr_financ),
            ("NR_INV_N_RES", c.nr_inv_n_res),
            ("NR_ENT_AB_PREV_COMPL", c.nr_ent_ab_prev_compl),
            ("NR_ENT_FC_PREV_COMPL", c.nr_ent_fc_prev_compl),
            ("NR_REG_PREV_SERV_PUB", c.nr_reg_prev_serv_pub),
            ("NR_SOC_SEG_RESEG", c.nr_soc_seg_reseg),
            ("NR_SOC_CAPTLZ_ARRENDM_MERC", c.nr_soc_captlz_arrendm_merc),
            ("NR_FDOS_CLUB_INV", c.nr_fdos_club_inv),
            ("NR_COTST_DISTR_FDO", c.nr_cotst_distr_fdo),
            ("NR_OUTROS_N_RELAC", c.nr_outros_n_relac),
        ]
        return (
            [f"{ind}    <NR_CLIENT>"]
            + [f"{inner}<{tag}>{val}</{tag}>" for tag, val in pairs]
            + [f"{ind}    </NR_CLIENT>"]
        )

    def _build_distr_patrim_lines(
        self, dp: Optional[PatrimonyDistribution], ind: str
    ) -> list[str]:
        """Build XML lines for the optional DISTR_PATRIM block.

        Parameters
        ----------
        dp : Optional[PatrimonyDistribution]
            Patrimony distribution model, or None if absent.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines, empty if dp is None.
        """
        if dp is None:
            return []
        inner = f"{ind}        "
        pairs = [
            ("PR_PF_PRIV_BANK", dp.pr_pf_priv_bank),
            ("PR_PF_VARJ", dp.pr_pf_varj),
            ("PR_PJ_N_FINANC_PRIV_BANK", dp.pr_pj_n_financ_priv_bank),
            ("PR_PJ_N_FINANC_VARJ", dp.pr_pj_n_financ_varj),
            ("PR_BNC_COMERC", dp.pr_bnc_comerc),
            ("PR_PJ_CORR_DIST", dp.pr_pj_corr_dist),
            ("PR_PJ_OUTR_FINANC", dp.pr_pj_outr_financ),
            ("PR_INV_N_RES", dp.pr_inv_n_res),
            ("PR_ENT_AB_PREV_COMPL", dp.pr_ent_ab_prev_compl),
            ("PR_ENT_FC_PREV_COMPL", dp.pr_ent_fc_prev_compl),
            ("PR_REG_PREV_SERV_PUB", dp.pr_reg_prev_serv_pub),
            ("PR_SOC_SEG_RESEG", dp.pr_soc_seg_reseg),
            ("PR_SOC_CAPTLZ_ARRENDM_MERC", dp.pr_soc_captlz_arrendm_merc),
            ("PR_FDOS_CLUB_INV", dp.pr_fdos_club_inv),
            ("PR_COTST_DISTR_FDO", dp.pr_cotst_distr_fdo),
            ("PR_OUTROS_N_RELAC", dp.pr_outros_n_relac),
        ]
        children = [
            f"{inner}<{tag}>{self._fmt_decimal(val, 1)}</{tag}>"
            for tag, val in pairs
            if val is not None
        ]
        return [f"{ind}    <DISTR_PATRIM>"] + children + [f"{ind}    </DISTR_PATRIM>"]

    def _build_risk_scalar_lines(self, row: PerfilMensalRow, ind: str) -> list[str]:
        """Build XML lines for optional VAR, vote and assembly text fields.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for text and scalar risk fields.
        """
        lines: list[str] = []
        for tag, val, max_len in [
            ("RESM_TEOR_VT_PROFRD", row.resm_teor_vt_profrd, 4000),
            ("JUST_SUM_VT_PROFRD", row.just_sum_vt_profrd, 4000),
        ]:
            if val is not None:
                lines.append(f"{ind}    <{tag}>{saxutils.escape(val[:max_len])}</{tag}>")
        if row.var_perc_pl is not None:
            lines.append(
                f"{ind}    <VAR_PERC_PL>"
                f"{self._fmt_decimal(row.var_perc_pl, 4)}"
                f"</VAR_PERC_PL>"
            )
        if row.mod_var_utiliz is not None:
            lines.append(f"{ind}    <MOD_VAR_UTILIZ>{row.mod_var_utiliz}</MOD_VAR_UTILIZ>")
        if row.praz_med_cart_tit is not None:
            lines.append(
                f"{ind}    <PRAZ_MED_CART_TIT>"
                f"{self._fmt_decimal(row.praz_med_cart_tit, 4)}"
                f"</PRAZ_MED_CART_TIT>"
            )
        if row.res_delib is not None:
            lines.append(
                f"{ind}    <RES_DELIB>{saxutils.escape(row.res_delib[:4000])}</RES_DELIB>"
            )
        return lines

    def _build_usd_flow_lines(self, row: PerfilMensalRow, ind: str) -> list[str]:
        """Build XML lines for the mandatory USD resource flow fields.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for TOTAL_RECURS_EXTER and TOTAL_RECURS_BR.
        """
        return [
            f"{ind}    <TOTAL_RECURS_EXTER>"
            f"{self._fmt_decimal(row.total_recurs_exter, 2)}"
            f"</TOTAL_RECURS_EXTER>",
            f"{ind}    <TOTAL_RECURS_BR>"
            f"{self._fmt_decimal(row.total_recurs_br, 2)}"
            f"</TOTAL_RECURS_BR>",
        ]

    def _build_variacao_vpc_lines(self, vpc: VarPercValCota, ind: str) -> list[str]:
        """Build XML lines for the VARIACAO_PERC_VAL_COTA block.

        Parameters
        ----------
        vpc : VarPercValCota
            Stress scenario block model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for the stress scenario and primitive risk factor list.
        """
        lines: list[str] = [
            f"{ind}    <VARIACAO_PERC_VAL_COTA>",
            f"{ind}        <VAL_PERCENT>"
            f"{self._fmt_decimal(vpc.val_percent, 2)}"
            f"</VAL_PERCENT>",
            f"{ind}        <LISTA_FATOR_PRIMIT_RISCO>",
        ]
        for fpr in vpc.lista_fator_primit_risco:
            lines += [
                f"{ind}            <FATOR_PRIMIT_RISCO>",
                f"{ind}                <NOME_FATOR_PRIMIT_RISCO>"
                f"{saxutils.escape(fpr.nome_fator_primit_risco)}"
                f"</NOME_FATOR_PRIMIT_RISCO>",
                f"{ind}                <CEN_UTIL>"
                f"{saxutils.escape(fpr.cen_util)}"
                f"</CEN_UTIL>",
                f"{ind}            </FATOR_PRIMIT_RISCO>",
            ]
        lines += [
            f"{ind}        </LISTA_FATOR_PRIMIT_RISCO>",
            f"{ind}    </VARIACAO_PERC_VAL_COTA>",
        ]
        return lines

    def _build_sensitivity_lines(self, row: PerfilMensalRow, ind: str) -> list[str]:
        """Build XML lines for the optional sensitivity scalar fields.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for stress-scenario and DV01-style sensitivity fields.
        """
        pairs: list[tuple[str, Optional[Decimal], int]] = [
            ("VAR_DIAR_PERC_COTA_FDO_PIOR_CEN_ESTRESS",
             row.var_diar_perc_cota_fdo_pior_cen_estress, 2),
            ("VAR_DIAR_PERC_PATRIM_FDO_VAR_N_TAXA_ANUAL",
             row.var_diar_perc_patrim_fdo_var_n_taxa_anual, 4),
            ("VAR_DIAR_PERC_PATRIM_FDO_VAR_N_TAXA_CAMBIO",
             row.var_diar_perc_patrim_fdo_var_n_taxa_cambio, 4),
            ("VAR_PATRIM_FDO_N_PRECO_ACOES",
             row.var_patrim_fdo_n_preco_acoes, 4),
        ]
        return [
            f"{ind}    <{tag}>{self._fmt_decimal(val, places)}</{tag}>"
            for tag, val, places in pairs
            if val is not None
        ]

    def _build_var_outros_lines(self, vo: VarOutros, ind: str) -> list[str]:
        """Build XML lines for the VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS block.

        Parameters
        ----------
        vo : VarOutros
            Other risk factor sensitivity model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for the other risk factor block.
        """
        return [
            f"{ind}    <VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS>",
            f"{ind}        <FATOR_RISCO_OUTROS>"
            f"{saxutils.escape(vo.fator_risco_outros[:400])}"
            f"</FATOR_RISCO_OUTROS>",
            f"{ind}        <VAL_PERCENT_OUTROS>"
            f"{self._fmt_decimal(vo.val_percent_outros, 4)}"
            f"</VAL_PERCENT_OUTROS>",
            f"{ind}    </VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS>",
        ]

    def _build_nominal_risk_lines(self, nb: NominalRiskBlock, ind: str) -> list[str]:
        """Build XML lines for the VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO block.

        Parameters
        ----------
        nb : NominalRiskBlock
            OTC derivatives notional block model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for notional risk factors.
        """
        lines: list[str] = [
            f"{ind}    <VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO>",
            f"{ind}        <VAL_COLATERAL>"
            f"{self._fmt_decimal(nb.val_colateral, 2)}"
            f"</VAL_COLATERAL>",
            f"{ind}        <LISTA_FATOR_RISCO_NOC>",
        ]
        for fnoc in nb.lista_fator_risco_noc:
            lines += [
                f"{ind}            <FATOR_RISCO_NOC>",
                f"{ind}                <NOME_FATOR_NOC>"
                f"{saxutils.escape(fnoc.nome_fator_noc)}"
                f"</NOME_FATOR_NOC>",
                f"{ind}                <VAL_FATOR_RISCO_NOC_LONG>"
                f"{fnoc.val_fator_risco_noc_long}"
                f"</VAL_FATOR_RISCO_NOC_LONG>",
                f"{ind}                <VAL_FATOR_RISCO_NOC_SHORT>"
                f"{fnoc.val_fator_risco_noc_short}"
                f"</VAL_FATOR_RISCO_NOC_SHORT>",
                f"{ind}            </FATOR_RISCO_NOC>",
            ]
        lines += [
            f"{ind}        </LISTA_FATOR_RISCO_NOC>",
            f"{ind}    </VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO>",
        ]
        return lines

    def _build_otc_lines(
        self, lista_otc: Optional[list[OtcOperation]], ind: str
    ) -> list[str]:
        """Build XML lines for the optional LISTA_OPER_CURS_MERC_BALCAO block.

        Parameters
        ----------
        lista_otc : Optional[list[OtcOperation]]
            List of OTC counterparties (up to 3), or None if absent.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines, empty if lista_otc is falsy.
        """
        if not lista_otc:
            return []
        lines: list[str] = [f"{ind}    <LISTA_OPER_CURS_MERC_BALCAO>"]
        for otc in lista_otc:
            lines += [
                f"{ind}        <OPER_CURS_MERC_BALCAO>",
                f"{ind}            <TP_PESSOA>{otc.tp_pessoa}</TP_PESSOA>",
                f"{ind}            <NR_PF_PJ_COMITENTE>"
                f"{otc.nr_pf_pj_comitente}"
                f"</NR_PF_PJ_COMITENTE>",
                f"{ind}            <PARTE_RELACIONADA>"
                f"{otc.parte_relacionada}"
                f"</PARTE_RELACIONADA>",
                f"{ind}            <VALOR_PARTE>"
                f"{self._fmt_decimal(otc.valor_parte, 1)}"
                f"</VALOR_PARTE>",
                f"{ind}        </OPER_CURS_MERC_BALCAO>",
            ]
        lines.append(f"{ind}    </LISTA_OPER_CURS_MERC_BALCAO>")
        return lines

    def _build_issuers_lines(
        self, lista_issuers: Optional[list[PrivateCreditIssuer]], ind: str
    ) -> list[str]:
        """Build XML lines for the optional LISTA_EMISSORES_TIT_CRED_PRIV block.

        Parameters
        ----------
        lista_issuers : Optional[list[PrivateCreditIssuer]]
            List of private credit issuers (up to 3), or None if absent.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines, empty if lista_issuers is falsy.
        """
        if not lista_issuers:
            return []
        lines: list[str] = [f"{ind}    <LISTA_EMISSORES_TIT_CRED_PRIV>"]
        for iss in lista_issuers:
            lines += [
                f"{ind}        <EMISSORES_TIT_CRED_PRIV>",
                f"{ind}            <TP_PESSOA_EMISSOR>"
                f"{iss.tp_pessoa_emissor}"
                f"</TP_PESSOA_EMISSOR>",
                f"{ind}            <NR_PF_PJ_EMISSOR>"
                f"{iss.nr_pf_pj_emissor}"
                f"</NR_PF_PJ_EMISSOR>",
                f"{ind}            <PARTE_RELACIONADA>"
                f"{iss.parte_relacionada}"
                f"</PARTE_RELACIONADA>",
                f"{ind}            <VALOR_PARTE>"
                f"{self._fmt_decimal(iss.valor_parte, 1)}"
                f"</VALOR_PARTE>",
                f"{ind}        </EMISSORES_TIT_CRED_PRIV>",
            ]
        lines.append(f"{ind}    </LISTA_EMISSORES_TIT_CRED_PRIV>")
        return lines

    def _build_performance_fee_lines(self, row: PerfilMensalRow, ind: str) -> list[str]:
        """Build XML lines for the optional performance-fee blocks.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.
        ind : str
            Base indentation string.

        Returns
        -------
        list[str]
            XML lines for VED_REGUL and RESP_VED_REGUL blocks.
        """
        lines: list[str] = []
        if row.ved_regul_cobr_taxa_perform is not None:
            lines.append(
                f"{ind}    <VED_REGUL_COBR_TAXA_PERFORM>"
                f"{row.ved_regul_cobr_taxa_perform}"
                f"</VED_REGUL_COBR_TAXA_PERFORM>"
            )
        if row.resp_ved_regul_cobr_taxa_perform is not None:
            pfd = row.resp_ved_regul_cobr_taxa_perform
            lines += [
                f"{ind}    <RESP_VED_REGUL_COBR_TAXA_PERFORM>",
                f"{ind}        <DATA_COTA_FUNDO>{pfd.data_cota_fundo}</DATA_COTA_FUNDO>",
                f"{ind}        <VAL_COTA_FUNDO>"
                f"{self._fmt_decimal(pfd.val_cota_fundo, 5)}"
                f"</VAL_COTA_FUNDO>",
                f"{ind}    </RESP_VED_REGUL_COBR_TAXA_PERFORM>",
            ]
        return lines

    # ------------------------------------------------------------------
    # XML parsing helpers
    # ------------------------------------------------------------------

    def _parse_xml_file(self, path: str) -> PerfilMensalDocument:
        """Parse a CVM Perfil Mensal XML file into a PerfilMensalDocument model.

        Parameters
        ----------
        path : str
            Path to the XML file.

        Returns
        -------
        PerfilMensalDocument
            Validated document model.

        Raises
        ------
        ValueError
            If the file cannot be read or parsed.
        """
        try:
            raw = Path(path).read_bytes()
            # defusedxml validates for XXE and billion-laughs attacks
            root = fromstring(raw)
        except Exception as exc:
            raise ValueError(f"Cannot parse XML file '{path}': {exc}") from exc
        return self._parse_xml_tree(root)

    def _parse_xml_tree(self, root: Any) -> PerfilMensalDocument:
        """Build a PerfilMensalDocument from an ElementTree root element.

        Parameters
        ----------
        root : Any
            Root element from defusedxml.ElementTree.fromstring.

        Returns
        -------
        PerfilMensalDocument
            Validated document model.
        """
        cab = self._find(root, "CAB_INFORM")
        header = DocumentHeader(
            cod_doc=int(self._text(cab, "COD_DOC")),
            dt_compt=self._text(cab, "DT_COMPT"),
            dt_gerac_arq=self._text(cab, "DT_GERAC_ARQ"),
            versao=self._text(cab, "VERSAO"),
        )
        perfil_mensal = self._find(root, "PERFIL_MENSAL")
        rows = [
            self._parse_xml_row(row_el)
            for row_el in self._findall(perfil_mensal, "ROW_PERFIL")
        ]
        return PerfilMensalDocument(header=header, rows=rows)

    def _parse_xml_row(self, row_el: Any) -> PerfilMensalRow:
        """Build a PerfilMensalRow from a ROW_PERFIL element.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        PerfilMensalRow
            Validated row model.
        """
        return PerfilMensalRow(
            cnpj_fdo=self._text(row_el, "CNPJ_FDO"),
            nr_client=self._parse_nr_client(row_el),
            distr_patrim=self._parse_distr_patrim(row_el),
            resm_teor_vt_profrd=self._opt_text(row_el, "RESM_TEOR_VT_PROFRD"),
            just_sum_vt_profrd=self._opt_text(row_el, "JUST_SUM_VT_PROFRD"),
            var_perc_pl=self._opt_decimal(row_el, "VAR_PERC_PL"),
            mod_var_utiliz=self._opt_text(row_el, "MOD_VAR_UTILIZ"),
            praz_med_cart_tit=self._opt_decimal(row_el, "PRAZ_MED_CART_TIT"),
            res_delib=self._opt_text(row_el, "RES_DELIB"),
            total_recurs_exter=self._parse_decimal(
                self._text(row_el, "TOTAL_RECURS_EXTER")
            ),
            total_recurs_br=self._parse_decimal(self._text(row_el, "TOTAL_RECURS_BR")),
            variacao_perc_val_cota=self._parse_variacao_vpc(row_el),
            var_diar_perc_cota_fdo_pior_cen_estress=self._opt_decimal(
                row_el, "VAR_DIAR_PERC_COTA_FDO_PIOR_CEN_ESTRESS"
            ),
            var_diar_perc_patrim_fdo_var_n_taxa_anual=self._opt_decimal(
                row_el, "VAR_DIAR_PERC_PATRIM_FDO_VAR_N_TAXA_ANUAL"
            ),
            var_diar_perc_patrim_fdo_var_n_taxa_cambio=self._opt_decimal(
                row_el, "VAR_DIAR_PERC_PATRIM_FDO_VAR_N_TAXA_CAMBIO"
            ),
            var_patrim_fdo_n_preco_acoes=self._opt_decimal(
                row_el, "VAR_PATRIM_FDO_N_PRECO_ACOES"
            ),
            variacao_diar_perc_patrim_fdo_var_n_outros=self._parse_var_outros(row_el),
            valor_noc_tot_contrat_deriv_mant_fdo=self._parse_nominal_risk(row_el),
            lista_oper_curs_merc_balcao=self._parse_otc_list(row_el),
            tot_ativos_p_relac=self._parse_decimal(
                self._text(row_el, "TOT_ATIVOS_P_RELAC")
            ),
            lista_emissores_tit_cred_priv=self._parse_issuers_list(row_el),
            tot_ativos_cred_priv=self._parse_decimal(
                self._text(row_el, "TOT_ATIVOS_CRED_PRIV")
            ),
            ved_regul_cobr_taxa_perform=self._opt_text(
                row_el, "VED_REGUL_COBR_TAXA_PERFORM"
            ),
            resp_ved_regul_cobr_taxa_perform=self._parse_performance_fee(row_el),
            montante_distrib=self._opt_decimal(row_el, "MONTANTE_DISTRIB"),
            inf_compl_perfil=self._opt_text(row_el, "INF_COMPL_PERFIL"),
        )

    def _parse_nr_client(self, row_el: Any) -> ClientCount:
        """Parse the NR_CLIENT block from a ROW_PERFIL element.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        ClientCount
            Validated client count model.
        """
        nr_el = self._find(row_el, "NR_CLIENT")
        return ClientCount(
            nr_pf_priv_bank=int(self._text(nr_el, "NR_PF_PRIV_BANK")),
            nr_pf_varj=int(self._text(nr_el, "NR_PF_VARJ")),
            nr_pj_n_financ_priv_bank=int(self._text(nr_el, "NR_PJ_N_FINANC_PRIV_BANK")),
            nr_pj_n_financ_varj=int(self._text(nr_el, "NR_PJ_N_FINANC_VARJ")),
            nr_bnc_comerc=int(self._text(nr_el, "NR_BNC_COMERC")),
            nr_pj_corr_dist=int(self._text(nr_el, "NR_PJ_CORR_DIST")),
            nr_pj_outr_financ=int(self._text(nr_el, "NR_PJ_OUTR_FINANC")),
            nr_inv_n_res=int(self._text(nr_el, "NR_INV_N_RES")),
            nr_ent_ab_prev_compl=int(self._text(nr_el, "NR_ENT_AB_PREV_COMPL")),
            nr_ent_fc_prev_compl=int(self._text(nr_el, "NR_ENT_FC_PREV_COMPL")),
            nr_reg_prev_serv_pub=int(self._text(nr_el, "NR_REG_PREV_SERV_PUB")),
            nr_soc_seg_reseg=int(self._text(nr_el, "NR_SOC_SEG_RESEG")),
            nr_soc_captlz_arrendm_merc=int(
                self._text(nr_el, "NR_SOC_CAPTLZ_ARRENDM_MERC")
            ),
            nr_fdos_club_inv=int(self._text(nr_el, "NR_FDOS_CLUB_INV")),
            nr_cotst_distr_fdo=int(self._text(nr_el, "NR_COTST_DISTR_FDO")),
            nr_outros_n_relac=int(self._text(nr_el, "NR_OUTROS_N_RELAC")),
        )

    def _parse_distr_patrim(self, row_el: Any) -> Optional[PatrimonyDistribution]:
        """Parse the optional DISTR_PATRIM block from a ROW_PERFIL element.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[PatrimonyDistribution]
            Validated model, or None if the block is absent.
        """
        dp_el = self._find_opt(row_el, "DISTR_PATRIM")
        if dp_el is None:
            return None
        return PatrimonyDistribution(
            pr_pf_priv_bank=self._opt_decimal(dp_el, "PR_PF_PRIV_BANK"),
            pr_pf_varj=self._opt_decimal(dp_el, "PR_PF_VARJ"),
            pr_pj_n_financ_priv_bank=self._opt_decimal(dp_el, "PR_PJ_N_FINANC_PRIV_BANK"),
            pr_pj_n_financ_varj=self._opt_decimal(dp_el, "PR_PJ_N_FINANC_VARJ"),
            pr_bnc_comerc=self._opt_decimal(dp_el, "PR_BNC_COMERC"),
            pr_pj_corr_dist=self._opt_decimal(dp_el, "PR_PJ_CORR_DIST"),
            pr_pj_outr_financ=self._opt_decimal(dp_el, "PR_PJ_OUTR_FINANC"),
            pr_inv_n_res=self._opt_decimal(dp_el, "PR_INV_N_RES"),
            pr_ent_ab_prev_compl=self._opt_decimal(dp_el, "PR_ENT_AB_PREV_COMPL"),
            pr_ent_fc_prev_compl=self._opt_decimal(dp_el, "PR_ENT_FC_PREV_COMPL"),
            pr_reg_prev_serv_pub=self._opt_decimal(dp_el, "PR_REG_PREV_SERV_PUB"),
            pr_soc_seg_reseg=self._opt_decimal(dp_el, "PR_SOC_SEG_RESEG"),
            pr_soc_captlz_arrendm_merc=self._opt_decimal(
                dp_el, "PR_SOC_CAPTLZ_ARRENDM_MERC"
            ),
            pr_fdos_club_inv=self._opt_decimal(dp_el, "PR_FDOS_CLUB_INV"),
            pr_cotst_distr_fdo=self._opt_decimal(dp_el, "PR_COTST_DISTR_FDO"),
            pr_outros_n_relac=self._opt_decimal(dp_el, "PR_OUTROS_N_RELAC"),
        )

    def _parse_variacao_vpc(self, row_el: Any) -> Optional[VarPercValCota]:
        """Parse the optional VARIACAO_PERC_VAL_COTA block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[VarPercValCota]
            Validated model, or None if the block is absent.
        """
        vpc_el = self._find_opt(row_el, "VARIACAO_PERC_VAL_COTA")
        if vpc_el is None:
            return None
        fprs = [
            PrimitiveRiskFactor(
                nome_fator_primit_risco=self._text(fpr_el, "NOME_FATOR_PRIMIT_RISCO"),
                cen_util=self._text(fpr_el, "CEN_UTIL"),
            )
            for fpr_el in self._findall(
                self._find(vpc_el, "LISTA_FATOR_PRIMIT_RISCO"),
                "FATOR_PRIMIT_RISCO",
            )
        ]
        return VarPercValCota(
            val_percent=self._parse_decimal(self._text(vpc_el, "VAL_PERCENT")),
            lista_fator_primit_risco=fprs,
        )

    def _parse_var_outros(self, row_el: Any) -> Optional[VarOutros]:
        """Parse the optional VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[VarOutros]
            Validated model, or None if the block is absent.
        """
        vo_el = self._find_opt(row_el, "VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS")
        if vo_el is None:
            return None
        return VarOutros(
            fator_risco_outros=self._text(vo_el, "FATOR_RISCO_OUTROS"),
            val_percent_outros=self._parse_decimal(
                self._text(vo_el, "VAL_PERCENT_OUTROS")
            ),
        )

    def _parse_nominal_risk(self, row_el: Any) -> Optional[NominalRiskBlock]:
        """Parse the optional VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[NominalRiskBlock]
            Validated model, or None if the block is absent.
        """
        noc_el = self._find_opt(row_el, "VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO")
        if noc_el is None:
            return None
        fnocs = [
            NominalRiskFactor(
                nome_fator_noc=self._text(fn_el, "NOME_FATOR_NOC"),
                val_fator_risco_noc_long=int(self._text(fn_el, "VAL_FATOR_RISCO_NOC_LONG")),
                val_fator_risco_noc_short=int(
                    self._text(fn_el, "VAL_FATOR_RISCO_NOC_SHORT")
                ),
            )
            for fn_el in self._findall(
                self._find(noc_el, "LISTA_FATOR_RISCO_NOC"), "FATOR_RISCO_NOC"
            )
        ]
        return NominalRiskBlock(
            val_colateral=self._parse_decimal(self._text(noc_el, "VAL_COLATERAL")),
            lista_fator_risco_noc=fnocs,
        )

    def _parse_otc_list(self, row_el: Any) -> Optional[list[OtcOperation]]:
        """Parse the optional LISTA_OPER_CURS_MERC_BALCAO block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[list[OtcOperation]]
            List of validated OTC operations, or None if the block is absent.
        """
        balcao_el = self._find_opt(row_el, "LISTA_OPER_CURS_MERC_BALCAO")
        if balcao_el is None:
            return None
        return [
            OtcOperation(
                tp_pessoa=self._text(op_el, "TP_PESSOA"),
                nr_pf_pj_comitente=self._text(op_el, "NR_PF_PJ_COMITENTE"),
                parte_relacionada=self._text(op_el, "PARTE_RELACIONADA"),
                valor_parte=self._parse_decimal(self._text(op_el, "VALOR_PARTE")),
            )
            for op_el in self._findall(balcao_el, "OPER_CURS_MERC_BALCAO")
        ]

    def _parse_issuers_list(self, row_el: Any) -> Optional[list[PrivateCreditIssuer]]:
        """Parse the optional LISTA_EMISSORES_TIT_CRED_PRIV block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[list[PrivateCreditIssuer]]
            List of validated issuers, or None if the block is absent.
        """
        emit_el = self._find_opt(row_el, "LISTA_EMISSORES_TIT_CRED_PRIV")
        if emit_el is None:
            return None
        return [
            PrivateCreditIssuer(
                tp_pessoa_emissor=self._text(em_el, "TP_PESSOA_EMISSOR"),
                nr_pf_pj_emissor=self._text(em_el, "NR_PF_PJ_EMISSOR"),
                parte_relacionada=self._text(em_el, "PARTE_RELACIONADA"),
                valor_parte=self._parse_decimal(self._text(em_el, "VALOR_PARTE")),
            )
            for em_el in self._findall(emit_el, "EMISSORES_TIT_CRED_PRIV")
        ]

    def _parse_performance_fee(self, row_el: Any) -> Optional[PerformanceFeeDetails]:
        """Parse the optional RESP_VED_REGUL_COBR_TAXA_PERFORM block.

        Parameters
        ----------
        row_el : Any
            ROW_PERFIL ElementTree element.

        Returns
        -------
        Optional[PerformanceFeeDetails]
            Validated model, or None if the block is absent.
        """
        perf_el = self._find_opt(row_el, "RESP_VED_REGUL_COBR_TAXA_PERFORM")
        if perf_el is None:
            return None
        return PerformanceFeeDetails(
            data_cota_fundo=self._text(perf_el, "DATA_COTA_FUNDO"),
            val_cota_fundo=self._parse_decimal(self._text(perf_el, "VAL_COTA_FUNDO")),
        )

    # ------------------------------------------------------------------
    # Flat dict / DataFrame helpers
    # ------------------------------------------------------------------

    def _records_to_df(
        self,
        records: Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame],
    ) -> pd.DataFrame:
        """Convert any supported record format to a flattened DataFrame.

        Parameters
        ----------
        records : Union[list[BaseModel], list[dict[str, Any]], pd.DataFrame]
            Source records.

        Returns
        -------
        pd.DataFrame
            Flattened DataFrame ready for CSV/Excel export.

        Raises
        ------
        TypeError
            If records is not a supported type.
        """
        if isinstance(records, pd.DataFrame):
            return records
        if not isinstance(records, list):
            raise TypeError(
                "records must be a list of BaseModel instances, dicts, or a DataFrame."
            )
        if not records:
            return pd.DataFrame()
        if isinstance(records[0], BaseModel):
            raw_dicts = [self._model_to_serialisable_dict(r) for r in records]
        elif isinstance(records[0], dict):
            raw_dicts = list(records)
        else:
            raise TypeError(
                "List elements must be BaseModel instances or dicts."
            )
        return pd.json_normalize(raw_dicts)

    def _model_to_serialisable_dict(self, model: BaseModel) -> dict[str, Any]:
        """Dump a Pydantic model to a flat dict suitable for CSV/Excel export.

        All nested dicts and lists are JSON-encoded as strings so every row
        occupies exactly one line. Scalar fields are kept as-is.

        Parameters
        ----------
        model : BaseModel
            Pydantic model instance.

        Returns
        -------
        dict[str, Any]
            Flat dict where complex values are JSON-encoded strings.
        """
        raw = model.model_dump(mode="json")
        return {
            k: json.dumps(v, ensure_ascii=False, default=str)
            if isinstance(v, (dict, list))
            else v
            for k, v in raw.items()
        }

    def _row_to_flat_dict(self, row: PerfilMensalRow) -> dict[str, Any]:
        """Flatten a PerfilMensalRow to a single-level dict for DataFrame output.

        Nested objects and lists are JSON-encoded strings; scalars are kept as-is.

        Parameters
        ----------
        row : PerfilMensalRow
            Validated row model.

        Returns
        -------
        dict[str, Any]
            Flat dict where complex values are JSON-encoded strings.
        """
        return self._model_to_serialisable_dict(row)

    def _read_tabular(self, path: str) -> pd.DataFrame:
        """Read a CSV or Excel file into a DataFrame.

        Parameters
        ----------
        path : str
            File path. Extension determines format (.csv → CSV, else Excel).

        Returns
        -------
        pd.DataFrame
            Loaded DataFrame.

        Raises
        ------
        ValueError
            If the extension is not supported.
        """
        suffix = Path(path).suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path, dtype=str, keep_default_na=False)
        raise ValueError(
            f"Unsupported file format '{suffix}'. Use .csv, .xlsx, or .xls."
        )

    def _flat_dict_to_row(self, flat: dict[str, Any]) -> PerfilMensalRow:
        """Reconstruct a PerfilMensalRow from a flat dict (as produced by from_xml → to_csv).

        JSON-encoded string values are decoded back to dicts/lists. Empty strings
        from CSV are normalised to None.

        Parameters
        ----------
        flat : dict[str, Any]
            Flat dict where complex fields are JSON-encoded strings.

        Returns
        -------
        PerfilMensalRow
            Validated row model.
        """
        decoded: dict[str, Any] = {}
        for k, v in flat.items():
            if isinstance(v, str):
                stripped = v.strip()
                if stripped in ("", "nan"):
                    decoded[k] = None
                elif stripped.startswith(("{", "[")):
                    try:
                        decoded[k] = json.loads(stripped)
                    except json.JSONDecodeError:
                        decoded[k] = stripped or None
                else:
                    decoded[k] = stripped
            else:
                decoded[k] = v
        return PerfilMensalRow(**decoded)

    def _unflatten(self, flat: dict[str, Any], sep: str = ".") -> dict[str, Any]:
        """Reconstruct a nested dict from dot-separated keys.

        Parameters
        ----------
        flat : dict[str, Any]
            Flat dict with dot-separated keys.
        sep : str, optional
            Key separator, by default '.'.

        Returns
        -------
        dict[str, Any]
            Nested dict.
        """
        result: dict[str, Any] = {}
        for key, val in flat.items():
            parts = key.split(sep)
            node = result
            for part in parts[:-1]:
                node = node.setdefault(part, {})
            node[parts[-1]] = val if val != "" else None
        return result

    def _decode_list_fields(self, d: dict[str, Any]) -> dict[str, Any]:
        """Recursively parse JSON-string values back to lists.

        Parameters
        ----------
        d : dict[str, Any]
            Nested dict potentially containing JSON-encoded list strings.

        Returns
        -------
        dict[str, Any]
            Dict with JSON strings decoded back to Python lists.
        """
        result: dict[str, Any] = {}
        for key, val in d.items():
            if isinstance(val, str):
                stripped = val.strip()
                if stripped.startswith("["):
                    try:
                        result[key] = json.loads(stripped)
                        continue
                    except json.JSONDecodeError:
                        pass
            if isinstance(val, dict):
                result[key] = self._decode_list_fields(val)
            else:
                result[key] = val
        return result

    # ------------------------------------------------------------------
    # Low-level XML element helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find(parent: Any, tag: str) -> Any:
        """Find a required child element by local tag name, ignoring namespace.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name (without namespace prefix).

        Returns
        -------
        Any
            Matched child element.

        Raises
        ------
        ValueError
            If the element is not found.
        """
        for child in parent:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == tag:
                return child
        raise ValueError(f"Required XML element <{tag}> not found.")

    @staticmethod
    def _find_opt(parent: Any, tag: str) -> Optional[Any]:
        """Find an optional child element by local tag name, returning None if absent.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name.

        Returns
        -------
        Optional[Any]
            Matched child element or None.
        """
        for child in parent:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == tag:
                return child
        return None

    @staticmethod
    def _findall(parent: Any, tag: str) -> list[Any]:
        """Find all child elements matching a local tag name.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name.

        Returns
        -------
        list[Any]
            List of matched child elements.
        """
        return [
            child
            for child in parent
            if (child.tag.split("}")[-1] if "}" in child.tag else child.tag) == tag
        ]

    @staticmethod
    def _text(parent: Any, tag: str) -> str:
        """Return the text content of a required child element.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name.

        Returns
        -------
        str
            Stripped text content.

        Raises
        ------
        ValueError
            If the element is not found or has no text.
        """
        for child in parent:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == tag:
                return (child.text or "").strip()
        raise ValueError(f"Required XML element <{tag}> not found.")

    @staticmethod
    def _opt_text(parent: Any, tag: str) -> Optional[str]:
        """Return the text content of an optional child element.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name.

        Returns
        -------
        Optional[str]
            Stripped text or None if element is absent.
        """
        for child in parent:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == tag:
                return (child.text or "").strip() or None
        return None

    @staticmethod
    def _parse_decimal(raw: str) -> Decimal:
        """Parse a CVM decimal string (comma as separator) to Decimal.

        Parameters
        ----------
        raw : str
            Raw string from XML (e.g. '12,34').

        Returns
        -------
        Decimal
            Parsed value.

        Raises
        ------
        ValueError
            If the string cannot be converted.
        """
        try:
            return Decimal(raw.replace(",", "."))
        except InvalidOperation as exc:
            raise ValueError(f"Cannot parse '{raw}' as a decimal number.") from exc

    @classmethod
    def _opt_decimal(cls, parent: Any, tag: str) -> Optional[Decimal]:
        """Parse an optional child element's text as a CVM decimal.

        Parameters
        ----------
        parent : Any
            Parent ElementTree element.
        tag : str
            Local XML tag name.

        Returns
        -------
        Optional[Decimal]
            Parsed value or None if element is absent.
        """
        raw = cls._opt_text(parent, tag)
        return cls._parse_decimal(raw) if raw is not None else None

    @staticmethod
    def _fmt_decimal(value: Decimal, places: int) -> str:
        """Format a Decimal for CVM XML output (comma as decimal separator).

        Parameters
        ----------
        value : Decimal
            Numeric value.
        places : int
            Number of decimal places.

        Returns
        -------
        str
            Formatted string (e.g. '12,3400').
        """
        return f"{value:.{places}f}".replace(".", ",")
