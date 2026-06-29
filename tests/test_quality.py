"""
Testes do GoldDataQuality.
Verifica cada check individualmente: duplicatas, nulls, negativo, zero.
"""

import pytest

GOLD_COLUMNS = [
    "uf_sigla", "uf_nome", "regiao_nome",
    "total_municipios", "data_processamento", "run_id",
]


def make_df(spark, rows):
    return spark.createDataFrame(rows, GOLD_COLUMNS)


###############################################################
# STATUS SUCCESS
###############################################################

def test_status_success_when_data_is_clean(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo",   "Sudeste", 645, "2025-01-01", "abc"),
        ("MG", "Minas Gerais","Sudeste", 853, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["status"] == "SUCCESS"
    assert report["duplicate_ufs"] == 0
    assert report["null_ufs"] == 0
    assert report["negative_totals"] == 0
    assert report["zero_totals"] == 0
    assert report["quality"] == 100.0


###############################################################
# DUPLICATAS
###############################################################

def test_detects_duplicate_ufs(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo", "Sudeste", 645, "2025-01-01", "abc"),
        ("SP", "São Paulo", "Sudeste", 100, "2025-01-01", "abc"),  # duplicado
    ])
    report = GoldDataQuality.check(df)

    assert report["duplicate_ufs"] > 0
    assert report["status"] != "SUCCESS"


###############################################################
# NULLS
###############################################################

def test_detects_null_uf_sigla(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        (None, "Desconhecido", "Sudeste", 100, "2025-01-01", "abc"),
        ("MG", "Minas Gerais", "Sudeste", 853, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["null_ufs"] > 0


def test_detects_empty_uf_sigla(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("  ", "Desconhecido", "Sudeste", 100, "2025-01-01", "abc"),
        ("MG", "Minas Gerais", "Sudeste", 853, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["null_ufs"] > 0


###############################################################
# NEGATIVO / ZERO
###############################################################

def test_detects_negative_total_municipios(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo", "Sudeste", -1, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["negative_totals"] > 0


def test_detects_zero_total_municipios(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo", "Sudeste", 0, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["zero_totals"] > 0


###############################################################
# QUALIDADE %
###############################################################

def test_quality_100_when_all_valid(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo",    "Sudeste", 645, "2025-01-01", "abc"),
        ("PR", "Paraná",       "Sul",     399, "2025-01-01", "abc"),
        ("GO", "Goiás",        "Centro-Oeste", 246, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    assert report["quality"] == 100.0


def test_quality_below_100_when_invalid_records(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo", "Sudeste", 645, "2025-01-01", "abc"),
        ("XX", "Inválido",  "Sudeste", 0,   "2025-01-01", "abc"),  # zero → inválido
    ])
    report = GoldDataQuality.check(df)

    assert report["quality"] < 100.0


###############################################################
# CAMPOS DO RELATÓRIO
###############################################################

def test_report_has_all_fields(spark):
    from pipelines.gold.ibge_gold_pipeline import GoldDataQuality

    df = make_df(spark, [
        ("SP", "São Paulo", "Sudeste", 645, "2025-01-01", "abc"),
    ])
    report = GoldDataQuality.check(df)

    expected_keys = {
        "total_ufs", "duplicate_ufs", "null_ufs",
        "negative_totals", "zero_totals",
        "invalid_records", "quality", "status",
    }
    assert expected_keys == set(report.keys())
