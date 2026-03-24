import unicodedata

def normalizar_coluna(nome):
    nome = nome.strip().lower().replace(" ", "_")
    nome = unicodedata.normalize('NFKD', nome).encode('ascii', 'ignore').decode('utf-8')
    nome = nome.replace("(", "").replace(")", "").replace("-", "_")
    return nome

def limpar_texto(texto):
    if isinstance(texto, str):
        texto = texto.strip().lower()
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    return texto

def converter_float(valor):
    if isinstance(valor, str):
        valor = valor.replace(",", ".")
    try:
        return float(valor)
    except:
        return None