import pandas as pd


def limpar_registros_incompletos(arquivo_entrada, arquivo_saida):
    """
    Carrega um arquivo CSV, remove registros com colunas vazias ou nulas,
    e salva um novo arquivo apenas com registros completos.

    :param arquivo_entrada: Nome do arquivo CSV de entrada (com separador '|')
    :param arquivo_saida: Nome do arquivo CSV de saída (também com separador '|')
    """
    # Carrega o arquivo CSV com o separador '|'
    print("Carregando o arquivo...")
    df = pd.read_csv(arquivo_entrada, sep='|')

    # Remove registros com valores nulos ou vazios em qualquer coluna
    print("Removendo registros incompletos...")
    df_cleaned = df.dropna()

    # Salva o resultado em um novo arquivo CSV
    print("Salvando arquivo limpo...")
    df_cleaned.to_csv(arquivo_saida, sep='|', index=False)

    print(f"Processo concluído! Arquivo limpo salvo como '{arquivo_saida}'.")


if __name__ == "__main__":
    # Nome do arquivo de entrada
    arquivo_entrada = 'E:/UCS Trab/Sem 14 - Prog Conc, Par, Distrib/devices.csv'
    # Nome do arquivo de saída
    arquivo_saida = 'E:/UCS Trab/Sem 14 - Prog Conc, Par, Distrib/devices_limpo.csv'

    # Executa a função para limpar registros incompletos
    limpar_registros_incompletos(arquivo_entrada, arquivo_saida)
