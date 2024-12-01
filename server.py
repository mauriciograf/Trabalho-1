import socket
import os
import time
import pandas as pd
from numba.openmp import openmp_context as openmp
from concurrent.futures import ThreadPoolExecutor

# Função de processamento sequencial
def processar_dados_sequencial(arquivo_entrada, arquivo_saida):
    print("Processando de forma sequencial...")
    start_time = time.time()

    df = pd.read_csv(arquivo_entrada, sep='|')

    resultados = []

    def filtrar_dados(tipo, condicao):
        resultado = df.query(condicao).copy()
        resultado['tipo'] = tipo
        return resultado

    resultados.append(filtrar_dados('temperatura > 40', 'temperatura > 40'))
    resultados.append(filtrar_dados('temperatura < 0', 'temperatura < 0'))
    resultados.append(filtrar_dados('umidade < 0%', 'umidade < 0'))
    resultados.append(filtrar_dados('umidade > 100%', 'umidade > 100'))
    resultados.append(filtrar_dados('luminosidade < 0 lux', 'luminosidade < 0'))

    df_resultado = pd.concat(resultados)

    # Reorganizar colunas e salvar o resultado
    colunas_interesse = ['device', 'temperatura', 'umidade', 'luminosidade', 'data', 'tipo']
    df_resultado = df_resultado[colunas_interesse]
    df_resultado.to_csv(arquivo_saida, sep='|', index=False)

    end_time = time.time()
    print(f"Tempo de processamento sequencial: {end_time - start_time:.4f} segundos")

# Função de processamento com OpenMP (Numba)
def processar_dados_openmp(arquivo_entrada, arquivo_saida, num_threads):
    print(f"Processando com OpenMP usando {num_threads} threads...")
    start_time = time.time()

    df = pd.read_csv(arquivo_entrada, sep='|')

    resultados = []

    with openmp(num_threads):
        def filtrar_dados(tipo, condicao):
            resultado = df.query(condicao).copy()
            resultado['tipo'] = tipo
            return resultado

        resultados.append(filtrar_dados('temperatura > 40', 'temperatura > 40'))
        resultados.append(filtrar_dados('temperatura < 0', 'temperatura < 0'))
        resultados.append(filtrar_dados('umidade < 0%', 'umidade < 0'))
        resultados.append(filtrar_dados('umidade > 100%', 'umidade > 100'))
        resultados.append(filtrar_dados('luminosidade < 0 lux', 'luminosidade < 0'))

    df_resultado = pd.concat(resultados)

    # Reorganizar colunas e salvar o resultado
    colunas_interesse = ['device', 'temperatura', 'umidade', 'luminosidade', 'data', 'tipo']
    df_resultado = df_resultado[colunas_interesse]
    df_resultado.to_csv(arquivo_saida, sep='|', index=False)

    end_time = time.time()
    print(f"Tempo de processamento com OpenMP: {end_time - start_time:.4f} segundos")

# Função de processamento com PThreads
def processar_dados_pthreads(arquivo_entrada, arquivo_saida, num_threads):
    print(f"Processando com PThreads usando {num_threads} threads...")
    start_time = time.time()

    df = pd.read_csv(arquivo_entrada, sep='|')

    def filtrar_dados(tipo, condicao):
        resultado = df.query(condicao).copy()
        resultado['tipo'] = tipo
        return resultado

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futuros = []
        futuros.append(executor.submit(filtrar_dados, 'temperatura > 40', 'temperatura > 40'))
        futuros.append(executor.submit(filtrar_dados, 'temperatura < 0', 'temperatura < 0'))
        futuros.append(executor.submit(filtrar_dados, 'umidade < 0%', 'umidade < 0'))
        futuros.append(executor.submit(filtrar_dados, 'umidade > 100%', 'umidade > 100'))
        futuros.append(executor.submit(filtrar_dados, 'luminosidade < 0 lux', 'luminosidade < 0'))

        resultados = [fut.result() for fut in futuros]

    df_resultado = pd.concat(resultados)

    # Reorganizar colunas e salvar o resultado
    colunas_interesse = ['device', 'temperatura', 'umidade', 'luminosidade', 'data', 'tipo']
    df_resultado = df_resultado[colunas_interesse]
    df_resultado.to_csv(arquivo_saida, sep='|', index=False)

    end_time = time.time()
    print(f"Tempo de processamento com PThreads: {end_time - start_time:.4f} segundos")

# Função principal do servidor
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 12345))
    server_socket.listen(10)
    print('Aguardando conexão do cliente...')

    while True:
        connection, client_address = server_socket.accept()
        try:
            print(f'Cliente conectado: {client_address}')

            # Recebe a escolha do cliente
            option = int(connection.recv(1024).decode())
            print(f"Opção escolhida pelo cliente: {option}")

            # Recebe o nome do arquivo
            file_name = connection.recv(1024).decode()
            print(f"Arquivo recebido: {file_name}")

            # Salva o arquivo recebido
            directory = "received_files"
            if not os.path.exists(directory):
                os.makedirs(directory)
            file_path = os.path.join(directory, f"{int(time.time())}_{file_name}")

            with open(file_path, 'wb') as file:
                while True:
                    data = connection.recv(1024)
                    if not data:
                        print("Transferência de arquivo concluída.")
                        break
                    file.write(data)

            # Processa o arquivo conforme a opção
            output_path = os.path.join(directory, f"processed_{file_name}")
            if option == 1:
                processar_dados_openmp(file_path, output_path, num_threads=2)
            elif option == 2:
                processar_dados_openmp(file_path, output_path, num_threads=4)
            elif option == 3:
                processar_dados_pthreads(file_path, output_path, num_threads=2)
            elif option == 4:
                processar_dados_pthreads(file_path, output_path, num_threads=4)
            elif option == 5:
                processar_dados_sequencial(file_path, output_path)
            else:
                print("Opção inválida.")

            print(f"Arquivo processado salvo em: {output_path}")

        except Exception as e:
            print(f"Erro: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    main()
