from framework.data_engine import DataEngine

engine = DataEngine()

# Caminho do arquivo que acabamos de baixar
json_path = "data/landing/ibge/municipios.json"

# Lendo o JSON diretamente com SQL
print("Lendo os primeiros 5 municípios do IBGE:")
df = engine.query(f"SELECT nome, id FROM read_json_auto('{json_path}') LIMIT 5").to_df()

print(df)
