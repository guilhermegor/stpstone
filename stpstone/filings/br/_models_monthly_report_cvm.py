"""Pydantic models for the CVM Monthly Profile (Perfil Mensal) regulatory report."""

import re
from decimal import Decimal
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, field_validator


PercentField = Annotated[Decimal, Field(ge=Decimal("0"), le=Decimal("100"))]


class DocumentHeader(BaseModel):
    """CAB_INFORM - document-level header block."""

    cod_doc: int = 40
    dt_compt: str
    dt_gerac_arq: str
    versao: str = "4.0"

    @field_validator("dt_compt")
    @classmethod
    def _check_dt_compt(cls, v: str) -> str:
        """Validate dt_compt format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value does not follow MM/AAAA format.
        """
        if not re.match(r"^\d{2}/\d{4}$", v):
            raise ValueError("dt_compt must follow MM/AAAA format.")
        return v

    @field_validator("dt_gerac_arq")
    @classmethod
    def _check_dt_gerac_arq(cls, v: str) -> str:
        """Validate dt_gerac_arq format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value does not follow DD/MM/AAAA format.
        """
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", v):
            raise ValueError("dt_gerac_arq must follow DD/MM/AAAA format.")
        return v


class ClientCount(BaseModel):
    """NR_CLIENT - count of clients by investor type (all mandatory, non-negative)."""

    nr_pf_priv_bank: int = Field(ge=0)
    nr_pf_varj: int = Field(ge=0)
    nr_pj_n_financ_priv_bank: int = Field(ge=0)
    nr_pj_n_financ_varj: int = Field(ge=0)
    nr_bnc_comerc: int = Field(ge=0)
    nr_pj_corr_dist: int = Field(ge=0)
    nr_pj_outr_financ: int = Field(ge=0)
    nr_inv_n_res: int = Field(ge=0)
    nr_ent_ab_prev_compl: int = Field(ge=0)
    nr_ent_fc_prev_compl: int = Field(ge=0)
    nr_reg_prev_serv_pub: int = Field(ge=0)
    nr_soc_seg_reseg: int = Field(ge=0)
    nr_soc_captlz_arrendm_merc: int = Field(ge=0)
    nr_fdos_club_inv: int = Field(ge=0)
    nr_cotst_distr_fdo: int = Field(ge=0)
    nr_outros_n_relac: int = Field(ge=0)


class PatrimonyDistribution(BaseModel):
    """DISTR_PATRIM - patrimony percentage by client type (entire block is optional)."""

    pr_pf_priv_bank: Optional[PercentField] = None
    pr_pf_varj: Optional[PercentField] = None
    pr_pj_n_financ_priv_bank: Optional[PercentField] = None
    pr_pj_n_financ_varj: Optional[PercentField] = None
    pr_bnc_comerc: Optional[PercentField] = None
    pr_pj_corr_dist: Optional[PercentField] = None
    pr_pj_outr_financ: Optional[PercentField] = None
    pr_inv_n_res: Optional[PercentField] = None
    pr_ent_ab_prev_compl: Optional[PercentField] = None
    pr_ent_fc_prev_compl: Optional[PercentField] = None
    pr_reg_prev_serv_pub: Optional[PercentField] = None
    pr_soc_seg_reseg: Optional[PercentField] = None
    pr_soc_captlz_arrendm_merc: Optional[PercentField] = None
    pr_fdos_club_inv: Optional[PercentField] = None
    pr_cotst_distr_fdo: Optional[PercentField] = None
    pr_outros_n_relac: Optional[PercentField] = None


class PrimitiveRiskFactor(BaseModel):
    """FATOR_PRIMIT_RISCO - one BM&FBOVESPA primitive risk factor entry."""

    nome_fator_primit_risco: Literal[
        "IBOVESPA", "JUROS-PRE", "CUPOM CAMBIAL", "DOLAR", "OUTROS"
    ]
    cen_util: str = Field(max_length=150)


class VarPercValCota(BaseModel):
    """VARIACAO_PERC_VAL_COTA - stress scenario block with primitive risk factors."""

    val_percent: Decimal
    lista_fator_primit_risco: list[PrimitiveRiskFactor] = Field(max_length=5)


class VarOutros(BaseModel):
    """VARIACAO_DIAR_PERC_PATRIM_FDO_VAR_N_OUTROS - sensitivity to a non-standard risk factor."""

    fator_risco_outros: str = Field(max_length=400)
    val_percent_outros: Decimal


class NominalRiskFactor(BaseModel):
    """FATOR_RISCO_NOC - one notional derivatives risk factor (long and short legs)."""

    nome_fator_noc: Literal[
        "IBOVESPA", "JUROS-PRE", "CUPOM CAMBIAL", "DOLAR", "OUTROS"
    ]
    val_fator_risco_noc_long: int = Field(ge=0)
    val_fator_risco_noc_short: int = Field(ge=0)


class NominalRiskBlock(BaseModel):
    """VALOR_NOC_TOT_CONTRAT_DERIV_MANT_FDO - OTC derivatives notional exposure block."""

    val_colateral: Decimal
    lista_fator_risco_noc: list[NominalRiskFactor] = Field(max_length=5)


class OtcOperation(BaseModel):
    """OPER_CURS_MERC_BALCAO - one top-3 OTC counterparty without central clearing."""

    tp_pessoa: Literal["PF", "PJ"]
    nr_pf_pj_comitente: str
    parte_relacionada: Literal["S", "N"]
    valor_parte: Decimal

    @field_validator("nr_pf_pj_comitente")
    @classmethod
    def _check_doc(cls, v: str) -> str:
        """Validate CPF or CNPJ format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value is not 11 (CPF) or 14 (CNPJ) digits with no punctuation.
        """
        if not re.match(r"^\d{11}$|^\d{14}$", v):
            raise ValueError(
                "nr_pf_pj_comitente must be 11 (CPF) or 14 (CNPJ) digits with no punctuation."
            )
        return v


class PrivateCreditIssuer(BaseModel):
    """EMISSORES_TIT_CRED_PRIV - one top-3 private credit issuer held by the fund class."""

    tp_pessoa_emissor: Literal["PF", "PJ"]
    nr_pf_pj_emissor: str
    parte_relacionada: Literal["S", "N"]
    valor_parte: Decimal

    @field_validator("nr_pf_pj_emissor")
    @classmethod
    def _check_doc(cls, v: str) -> str:
        """Validate CPF or CNPJ format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value is not 11 (CPF) or 14 (CNPJ) digits with no punctuation.
        """
        if not re.match(r"^\d{11}$|^\d{14}$", v):
            raise ValueError(
                "nr_pf_pj_emissor must be 11 (CPF) or 14 (CNPJ) digits with no punctuation."
            )
        return v


class PerformanceFeeDetails(BaseModel):
    """RESP_VED_REGUL_COBR_TAXA_PERFORM - date and NAV of the last performance fee charge."""

    data_cota_fundo: str
    val_cota_fundo: Decimal

    @field_validator("data_cota_fundo")
    @classmethod
    def _check_date(cls, v: str) -> str:
        """Validate date format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value does not follow DD/MM/AAAA format.
        """
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", v):
            raise ValueError("data_cota_fundo must follow DD/MM/AAAA format.")
        return v


class PerfilMensalRow(BaseModel):
    """ROW_PERFIL - one fund-class monthly profile entry."""

    cnpj_fdo: str
    nr_client: ClientCount
    distr_patrim: Optional[PatrimonyDistribution] = None
    resm_teor_vt_profrd: Optional[str] = Field(default=None, max_length=4000)
    just_sum_vt_profrd: Optional[str] = Field(default=None, max_length=4000)
    var_perc_pl: Optional[Decimal] = None
    mod_var_utiliz: Optional[Literal["1", "2", "3"]] = None
    praz_med_cart_tit: Optional[Decimal] = None
    res_delib: Optional[str] = Field(default=None, max_length=4000)
    total_recurs_exter: Decimal
    total_recurs_br: Decimal
    variacao_perc_val_cota: Optional[VarPercValCota] = None
    var_diar_perc_cota_fdo_pior_cen_estress: Optional[Decimal] = None
    var_diar_perc_patrim_fdo_var_n_taxa_anual: Optional[Decimal] = None
    var_diar_perc_patrim_fdo_var_n_taxa_cambio: Optional[Decimal] = None
    var_patrim_fdo_n_preco_acoes: Optional[Decimal] = None
    variacao_diar_perc_patrim_fdo_var_n_outros: Optional[VarOutros] = None
    valor_noc_tot_contrat_deriv_mant_fdo: Optional[NominalRiskBlock] = None
    lista_oper_curs_merc_balcao: Optional[list[OtcOperation]] = Field(
        default=None, max_length=3
    )
    tot_ativos_p_relac: Decimal
    lista_emissores_tit_cred_priv: Optional[list[PrivateCreditIssuer]] = Field(
        default=None, max_length=3
    )
    tot_ativos_cred_priv: Decimal
    ved_regul_cobr_taxa_perform: Optional[Literal["S", "N"]] = None
    resp_ved_regul_cobr_taxa_perform: Optional[PerformanceFeeDetails] = None
    montante_distrib: Optional[Decimal] = None
    inf_compl_perfil: Optional[str] = Field(default=None, max_length=500)

    @field_validator("cnpj_fdo")
    @classmethod
    def _check_cnpj(cls, v: str) -> str:
        """Validate CNPJ format.

        Parameters
        ----------
        v : str
            Value to validate.

        Returns
        -------
        str
            Validated value.

        Raises
        ------
        ValueError
            If value is not exactly 14 digits with no punctuation.
        """
        if not re.match(r"^\d{14}$", v):
            raise ValueError("cnpj_fdo must be exactly 14 digits with no punctuation.")
        return v


class PerfilMensalDocument(BaseModel):
    """DOC_ARQ - complete CVM Monthly Profile XML document."""

    header: DocumentHeader
    rows: list[PerfilMensalRow]
