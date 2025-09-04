"""
Investment funds bylaws from brazilian SEC (CVM).

The CVM (Comissão Valores Mobiliários) is the Brazilian Securities Exchange Commission, 
which is responsible for regulating the securities market in Brazil.

The CVM has a website where you can find information about investment funds, such as their name, 
employer identification number (EIN/CNPJ), and other relevant details.
"""

from stpstone.ingestion.countries.br.bylaws.investment_funds_bylaws import InvestmentFunds


cls_ = InvestmentFunds(
    list_apps=[
        "DOC_REGUL_8557_122868_2025_02.pdf",
        "DOC_REGUL_10882_125278_2025_03.pdf",
        "DOC_REGUL_10980_124649_2025_03.pdf",
        "DOC_REGUL_11520_124851_2025_03.pdf",
        # "DOC_REGUL_13784_125081_2025_03.pdf",
        # "DOC_REGUL_14532_121734_2025_02.pdf",
        # "DOC_REGUL_15638_126987_2025_03.pdf",
        # "DOC_REGUL_15640_125039_2025_03.pdf",
        # "DOC_REGUL_16323_119980_2025_02.pdf",
        # "DOC_REGUL_16333_125043_2025_03.pdf",
        # "DOC_REGUL_16367_125082_2025_03.pdf",
        # "DOC_REGUL_16476_120375_2025_02.pdf",
        # "DOC_REGUL_16551_125463_2025_03.pdf",
        # "DOC_REGUL_17124_121158_2025_02.pdf",
        # "DOC_REGUL_17715_120602_2025_02.pdf",
        # "DOC_REGUL_17805_120007_2025_02.pdf",
        # "DOC_REGUL_17840_125261_2025_03.pdf",
        # "DOC_REGUL_18235_125492_2025_03.pdf",
        # "DOC_REGUL_19078_124930_2025_03.pdf",
        # "DOC_REGUL_19130_126121_2025_03.pdf",
        # "DOC_REGUL_19498_121914_2025_02.pdf",
        # "DOC_REGUL_19667_122068_2025_02.pdf",
        # "DOC_REGUL_20214_125080_2025_03.pdf",
        # "DOC_REGUL_22448_120161_2025_02.pdf",
        # "DOC_REGUL_23008_119976_2025_02.pdf",
        # "DOC_REGUL_23639_121700_2025_02.pdf",
        # "DOC_REGUL_24094_120932_2025_02.pdf",
    ], 
    int_pages_join=3,
    logger=None,
    cls_db=None
)

df_ = cls_.run()
print(f"DF INVESTMENT FUNDS BYLAWS: \n{df_}")
df_.info()